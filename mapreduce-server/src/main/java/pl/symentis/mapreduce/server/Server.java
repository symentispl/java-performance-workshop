package pl.symentis.mapreduce.server;

import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
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
    private final Path jobsDir;
    private final WatchService watchService;
    private final WatchKey watchKey;
    private final ScheduledExecutorService executorService;
    private final Gson gson;
    private final MapReduce mapReduce;

    public Server(
            Path jobsDir,
            WatchService watchService,
            WatchKey watchKey,
            ScheduledExecutorService executorService,
            Gson gson,
            MapReduce mapReduce) {

        this.jobsDir = jobsDir;
        this.watchService = watchService;
        this.watchKey = watchKey;
        this.executorService = executorService;
        this.gson = gson;
        this.mapReduce = mapReduce;
    }

    private static Job loadJob(Path codeUri, Map<String, String> context) {
        try {
            var absolutePath = codeUri.toAbsolutePath();
            if (!Files.isRegularFile(absolutePath)) {
                throw new FileNotFoundException("file doesn't exist: " + absolutePath);
            }
            var url = absolutePath.toUri().toURL();
            LOG.debug("loading job from code url {}", url);
            ClassLoader jobClassLoader = URLClassLoader.newInstance(new URL[] {url});
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
        LOG.info("Map/reduce server started...");
        executorService.scheduleAtFixedRate(this::watchForJobs, 0, 1, TimeUnit.SECONDS);
    }

    public void shutdown() throws IOException {

        if (!executorService.isShutdown()) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        watchKey.cancel();
        watchService.close();
    }

    public Path jobsDir() {
        return jobsDir;
    }

    private void watchForJobs() {
        LOG.debug("polling watch key {} for filesystem modifications", watchKey.watchable());
        var watchEvents = watchKey.pollEvents();
        for (WatchEvent<?> watchEvent : watchEvents) {
            Path context = (Path) watchEvent.context();
            LOG.debug("new file {}", context);
            if (context.getFileName().toString().endsWith(".json")) {
                try (var reader = Files.newBufferedReader(jobsDir.resolve(context))) {
                    var jobDefinition = gson.fromJson(reader, JobDefinition.class);
                    LOG.debug("loaded new job definition {}", jobDefinition);
                    var job = loadJob(Paths.get(jobDefinition.getCodeUri()), jobDefinition.getContext());
                    LOG.debug("loaded new job {}", job);
                    executorService.submit(() -> {
                        LOG.debug("running job {}", job);
                        var output = new HashMap<String, Long>();
                        mapReduce.run(job.input(), job.mapper(), job.reducer(), output::put);
                    });
                    Files.deleteIfExists(context);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
