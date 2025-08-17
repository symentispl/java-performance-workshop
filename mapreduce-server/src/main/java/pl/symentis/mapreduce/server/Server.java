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
import java.nio.file.Paths;
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

    public Server(Javalin app, int port, ScheduledExecutorService executorService, Gson gson, MapReduce mapReduce) {

        this.app = app;
        this.port = port;
        this.executorService = executorService;
        this.gson = gson;
        this.mapReduce = mapReduce;

        this.app.post("/jobs", this::handleJobRequest);
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

    public void shutdown() throws IOException {
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

    private void handleJobRequest(Context ctx) {
        try {
            var jobDefinition = gson.fromJson(ctx.body(), JobDefinition.class);
            LOG.info("Received new job request: {}", jobDefinition);

            var job = loadJob(Paths.get(jobDefinition.getCodeUri()), jobDefinition.getContext());
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
            LOG.error("Error processing job request", e);
            ctx.status(500).result("Internal server error: " + e.getMessage());
        }
    }
}
