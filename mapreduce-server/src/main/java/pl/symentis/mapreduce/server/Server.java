package pl.symentis.mapreduce.server;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.json.JsonMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.core.Job;
import pl.symentis.mapreduce.core.JobFactory;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.offheap.LongSerializationStrategy;
import pl.symentis.mapreduce.offheap.StringSerializationStrategy;
import pl.symentis.mapreduce.rocksdb.RocksDBMapperOutput;

class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    static Server create(int port, MapReduce mapReduce, Path jobsDir) {
        LOG.info("initializing HTTP server on port {}", port);
        var gson = new Gson();
        var javalin = Javalin.create(config -> config.jsonMapper(new GsonMapper(gson)));

        var executorService =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() + 1);

        try {
            Files.createDirectories(jobsDir);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to create jobs storage directory %s".formatted(jobsDir.toAbsolutePath()), e);
        }

        return new Server(javalin, port, executorService, mapReduce, jobsDir);
    }

    private final Javalin app;
    private final int port;
    private final ScheduledExecutorService executorService;
    private final MapReduce mapReduce;
    private final Path jobsDir;
    private final ConcurrentHashMap<String, JobResults> jobResults = new ConcurrentHashMap<>();

    public Server(Javalin app, int port, ScheduledExecutorService executorService, MapReduce mapReduce, Path jobsDir) {
        this.app = app;
        this.port = port;
        this.executorService = executorService;
        this.mapReduce = mapReduce;
        this.jobsDir = jobsDir;
    }

    public void start() {
        LOG.info("starting Map/Reduce HTTP server on port {}", port);

        this.app.put("/jobs/", this::createJob);
        this.app.post("/jobs/{job-id}", this::submitJob);
        this.app.get("/jobs/{job-id}", this::jobStatus);
        this.app.delete("/jobs/{job-id}", this::deleteJob);

        app.start(port);
    }

    public void stop() throws IOException {
        LOG.info("shutting down Map/reduce HTTP server");
        app.stop();

        if (!executorService.isShutdown()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        mapReduce.shutdown();
    }

    private void createJob(Context ctx) {
        try {
            var jobId = UUID.randomUUID().toString();
            LOG.debug("creating new job with id {}", jobId);

            var jobDir = jobsDir.resolve(jobId);
            Files.createDirectories(jobDir);

            for (var uploadedFile : ctx.uploadedFiles()) {
                var filename = uploadedFile.filename();
                var targetFile = jobDir.resolve(filename);

                try (var inputStream = uploadedFile.content()) {
                    Files.copy(inputStream, targetFile, StandardCopyOption.REPLACE_EXISTING);
                    LOG.debug("uploaded file {} for job {}", filename, jobId);
                }
            }

            var jobUrlPath = "/jobs/%s".formatted(jobId);
            ctx.status(201).header("Location", jobUrlPath).json(new NewJobResponse(jobId, jobUrlPath));
        } catch (Exception e) {
            LOG.error("error processing job file upload", e);
            ctx.status(500).json(new ErrorResponse("Internal server error: " + e.getMessage()));
        }
    }

    private void submitJob(Context ctx) {
        try {
            var jobId = ctx.pathParam("job-id");
            var jobDefinition = ctx.bodyAsClass(SubmitJobRequest.class);
            LOG.info("received job execution request for job {} with parameters {} ", jobId, jobDefinition);

            var jobDir = jobsDir.resolve(jobId);
            if (!Files.exists(jobDir)) {
                ctx.status(404).json(new ErrorResponse("Job %s not found".formatted(jobId)));
                return;
            }

            var context = new HashMap<>(jobDefinition.context());

            Path codeJar;
            if (jobDefinition.codeUri() != null) {
                codeJar = jobDir.resolve(jobDefinition.codeUri());
            } else {
                try (var files = Files.list(jobDir)) {
                    codeJar = files.filter(f -> f.toString().endsWith(".jar"))
                            .findFirst()
                            .orElse(null);
                }
            }

            if (codeJar == null || !Files.exists(codeJar)) {
                ctx.status(400)
                        .json(new ErrorResponse("No code jar file %s found for job %s ".formatted(codeJar, jobId)));
                return;
            }

            for (Map.Entry<String, String> entry : context.entrySet()) {
                String relativePath = entry.getValue();
                if (!relativePath.startsWith("/") && !relativePath.contains(":")) {
                    Path absolutePath = jobDir.resolve(relativePath);
                    if (Files.exists(absolutePath)) {
                        context.put(entry.getKey(), absolutePath.toString());
                    }
                }
            }

            var job = loadJob(codeJar, context);
            if (job != null) {
                // Store initial status
                jobResults.put(jobId, new JobResults(JobStatus.RUNNING, Map.of()));

                executorService.submit(() -> {
                    LOG.debug("submitting job {}", job);
                    var output = new HashMap<String, Long>();
                    try {
                        mapReduce.run(job.input(), job.mapper(), job.reducer(), output::put);
                        LOG.debug("job completed with {} results", output.size());
                        jobResults.put(jobId, new JobResults(JobStatus.COMPLETED, output));
                    } catch (Exception e) {
                        LOG.error("job failed", e);
                        jobResults.put(jobId, new JobResults(JobStatus.FAILED, Map.of()));
                    }
                });
                ctx.status(202).json(new SubmitJobResponse("Job accepted for processing"));
            } else {
                ctx.status(400).json(new ErrorResponse("Failed to load job %s from %s".formatted(jobId, codeJar)));
            }
        } catch (Exception e) {
            LOG.error("Error processing job execution request", e);
            ctx.status(500).json(new ErrorResponse("Internal server error: %s".formatted(e.getMessage())));
        }
    }

    private void jobStatus(Context ctx) {
        var jobId = ctx.pathParam("job-id");
        var results = jobResults.get(jobId);

        if (results != null) {
            ctx.json(results);
        } else {
            ctx.status(404).json(new ErrorResponse("Job %s not found".formatted(jobId)));
        }
    }

    private void deleteJob(Context ctx) {
        var jobId = ctx.pathParam("job-id");
        var results = jobResults.remove(jobId);

        if (results != null) {
            // Clean up job files from disk
            var jobDir = jobsDir.resolve(jobId);
            try {
                if (Files.exists(jobDir)) {
                    Files.walk(jobDir)
                            .sorted(Comparator.reverseOrder()) // Delete files before directories
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    LOG.warn("Failed to delete {}", path, e);
                                }
                            });
                }
                LOG.debug("Cleaned up job {} files and results", jobId);
            } catch (IOException e) {
                LOG.error("Error cleaning up job {} files", jobId, e);
            }

            ctx.json(results);
        } else {
            ctx.status(404).json(new ErrorResponse("Job %s not found".formatted(jobId)));
        }
    }

    private static Job loadJob(Path codeUri, Map<String, String> context) {
        try {
            var absolutePath = codeUri.toAbsolutePath();
            if (!Files.isRegularFile(absolutePath)) {
                throw new FileNotFoundException("File %s doesn't exist".formatted(absolutePath));
            }
            var url = absolutePath.toUri().toURL();
            LOG.debug("loading job from code url {}", url);
            var jobClassLoader = URLClassLoader.newInstance(new URL[] {url});
            var jobServiceLoader = ServiceLoader.load(JobFactory.class, jobClassLoader);
            var first = jobServiceLoader.findFirst();
            if (first.isPresent()) {
                var job = first.get();
                LOG.debug("loaded job code {}", job);
                return job.create(context);
            } else {
                LOG.warn("job service not found");
            }
        } catch (Throwable e) {
            LOG.error("job failed", e);
        }
        return null;
    }

    private static class GsonMapper implements JsonMapper {
        private final Gson gson;

        public GsonMapper(Gson gson) {
            this.gson = gson;
        }

        @Override
        public String toJsonString(Object obj, java.lang.reflect.Type type) {
            return gson.toJson(obj, type);
        }

        @Override
        public <T> T fromJsonString(String json, java.lang.reflect.Type targetType) {
            return gson.fromJson(json, targetType);
        }
    }

    static class Builder {

        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private int port = 8080;
        private Path jobsDir;

        Builder port(int port) {
            this.port = port;
            return this;
        }

        Builder jobsDir(Path jobsDir) {
            this.jobsDir = jobsDir;
            return this;
        }

        Server build() throws IOException {

            LOG.info("initializing map reduce framework");
            var rocksDbPath = jobsDir.resolve("rocksdb");
            Files.createDirectories(rocksDbPath);
            var mapReduce = new BatchingMapReduce.Builder()
                    .withBatchSize(1000)
                    .withPhaserMaxTasks(10000)
                    .withThreadPoolSize(Runtime.getRuntime().availableProcessors())
                    .withMapperOutputSupplier(() -> new RocksDBMapperOutput<>(
                            rocksDbPath, new StringSerializationStrategy(), new LongSerializationStrategy()))
                    .build();

            return create(port, mapReduce, jobsDir);
        }
    }
}
