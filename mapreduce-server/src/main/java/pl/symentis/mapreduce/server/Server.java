package pl.symentis.mapreduce.server;

import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.core.Job;
import pl.symentis.mapreduce.core.JobFactory;
import pl.symentis.mapreduce.core.MapReduce;

class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private final Javalin app;
    private final int port;
    private final ScheduledExecutorService executorService;
    private final Gson gson;
    private final MapReduce mapReduce;
    private final Path jobsDir;

    public Server(
            Javalin app,
            int port,
            ScheduledExecutorService executorService,
            Gson gson,
            MapReduce mapReduce,
            Path jobsDir) {

        this.app = app;
        this.port = port;
        this.executorService = executorService;
        this.gson = gson;
        this.mapReduce = mapReduce;
        this.jobsDir = jobsDir;

        try {
            Files.createDirectories(jobsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create jobs storage directory", e);
        }

        this.app.put("/jobs/{job-id}", this::handleJobFileUpload);
        this.app.post("/jobs/{job-id}", this::handleJobExecution);
    }

    private static Job loadJob(Path codeUri, Map<String, String> context) {
        try {
            var absolutePath = codeUri.toAbsolutePath();
            if (!Files.isRegularFile(absolutePath)) {
                throw new FileNotFoundException("file doesn't exist: " + absolutePath);
            }
            var url = absolutePath.toUri().toURL();
            LOG.debug("loading job from code url {}", url);
            var jobClassLoader = URLClassLoader.newInstance(new URL[] {url});
            var jobServiceLoader = ServiceLoader.load(JobFactory.class, jobClassLoader);
            var first = jobServiceLoader.findFirst();
            if (first.isPresent()) {
                var job = first.get();
                LOG.debug("loaded job code", job);
                return job.create(context);
            } else {
                LOG.warn("job service not found");
            }
        } catch (Throwable e) {
            LOG.error("job failed", e);
        }
        return null;
    }

    public void start() {
        LOG.info("Starting Map/reduce HTTP server on port {}...", port);
        app.start(port);
    }

    public void stop() throws IOException {
        LOG.info("Shutting down Map/reduce HTTP server...");
        app.stop();

        if (!executorService.isShutdown()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void handleJobFileUpload(Context ctx) {
        try {
            var jobId = ctx.pathParam("job-id");
            LOG.debug("received files upload for job: {}", jobId);

            var jobDir = jobsDir.resolve(jobId);
            if (Files.isDirectory(jobDir)) {
                ctx.status(400).result("Job already exists");
                return;
            }

            Files.createDirectories(jobDir);

            for (var uploadedFile : ctx.uploadedFiles()) {
                var filename = uploadedFile.filename();
                var targetFile = jobDir.resolve(filename);

                try (var inputStream = uploadedFile.content()) {
                    Files.copy(inputStream, targetFile, StandardCopyOption.REPLACE_EXISTING);
                    LOG.debug("uploaded file: {} for job: {}", filename, jobId);
                }
            }
            ctx.status(200).result("Files uploaded successfully for job: " + jobId);
        } catch (Exception e) {
            LOG.error("Error processing job file upload", e);
            ctx.status(500).result("Internal server error: " + e.getMessage());
        }
    }

    private void handleJobExecution(Context ctx) {
        try {
            String jobId = ctx.pathParam("job-id");
            var jobDefinition = gson.fromJson(ctx.body(), JobDefinition.class);
            LOG.info("Received job execution request for job: {} with definition: {}", jobId, jobDefinition);

            Path jobDir = jobsDir.resolve(jobId);
            if (!Files.exists(jobDir)) {
                ctx.status(404).result("Job directory not found: " + jobId);
                return;
            }

            Map<String, String> context = new HashMap<>(jobDefinition.getContext());

            Path codeJar = null;
            if (jobDefinition.getCodeUri() != null) {
                codeJar = jobDir.resolve(jobDefinition.getCodeUri());
            } else {
                try (var files = Files.list(jobDir)) {
                    codeJar = files.filter(f -> f.toString().endsWith(".jar"))
                            .findFirst()
                            .orElse(null);
                }
            }

            if (codeJar == null || !Files.exists(codeJar)) {
                ctx.status(400).result("No JAR file found for job: " + jobId);
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
                executorService.submit(() -> {
                    LOG.info("Executing job: {}", job);
                    var output = new HashMap<String, Long>();
                    mapReduce.run(job.input(), job.mapper(), job.reducer(), output::put);
                    LOG.info("Job completed with {} results", output.size());
                });
                ctx.status(202).result("Job accepted for processing");
            } else {
                ctx.status(400).result("Failed to load job");
            }
        } catch (Exception e) {
            LOG.error("Error processing job execution request", e);
            ctx.status(500).result("Internal server error: " + e.getMessage());
        }
    }
}
