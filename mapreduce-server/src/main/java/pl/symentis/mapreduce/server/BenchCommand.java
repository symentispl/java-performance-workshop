package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "bench")
public class BenchCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BenchCommand.class);

    @Option(name = "--server-url")
    @Required
    private String serverUrl = "http://localhost:8080";

    @Option(name = "--job-interval-ms")
    private int jobIntervalMillis = 200;

    @Override
    public void run() {
        var client = new MapReduceServerClient(serverUrl);
        var scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        var jobProcessor = Executors.newCachedThreadPool();

        LOG.debug("starting benchmark with job interval {}ms", jobIntervalMillis);

        // Schedule job submissions at fixed intervals to avoid coordinated omission
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        submitJobAsync(client, jobProcessor);
                    } catch (Exception e) {
                        LOG.error("Error scheduling job submission", e);
                    }
                },
                0,
                jobIntervalMillis,
                TimeUnit.MILLISECONDS);

        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.debug("shutting down benchmark");
            scheduler.shutdown();
            jobProcessor.shutdown();
            try {
                client.close();
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!jobProcessor.awaitTermination(30, TimeUnit.SECONDS)) {
                    jobProcessor.shutdownNow();
                }
            } catch (Exception e) {
                LOG.error("error during benchmark shutdown", e);
            }
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOG.warn("benchmark interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private void submitJobAsync(MapReduceServerClient client, ExecutorService jobProcessor) {
        CompletableFuture.runAsync(
                () -> {
                    try {
                        processJob(client);
                    } catch (Exception e) {
                        LOG.error("error processing job", e);
                    }
                },
                jobProcessor);
    }

    private void processJob(MapReduceServerClient client) throws Exception {
        var jarFile = Paths.get("/app/mapreduce-wordcount-bundle.jar");
        var dataFile = Paths.get("/resources/big.txt");
        var jobParameters = Map.of("filename", "big.txt");

        long startTime = System.currentTimeMillis();

        JobResults results = null;
        String jobId = null;
        try {
            jobId = client.submitJob(jarFile, dataFile, jarFile.getFileName().toString(), jobParameters);
            LOG.debug("submitted job {}", jobId);

            results = waitForJobCompletion(client, jobId);

        } finally {
            long completionTime = System.currentTimeMillis();
            cleanupJob(client, results, jobId, completionTime, startTime);
        }
    }

    private static void cleanupJob(
            MapReduceServerClient client, JobResults results, String jobId, long completionTime, long startTime) {
        if (jobId != null) {
            LOG.debug(
                    "Job {} completed in {}ms with status {}, and results count {}",
                    jobId,
                    completionTime - startTime,
                    results.status(),
                    results.results().size());

            // Clean up job by deleting it
            try {
                client.deleteJob(jobId);
                LOG.debug("cleaned up job {}", jobId);
            } catch (Exception e) {
                LOG.warn("failed to clean up job {}", jobId, e);
            }
        } else {
            LOG.error("Job {} failed or timed out after {}ms", jobId, completionTime - startTime);
        }
    }

    private JobResults waitForJobCompletion(MapReduceServerClient client, String jobId) {
        var retryPolicy = RetryPolicy.<JobResults>builder()
                .handle(Exception.class)
                .handleResultIf(result -> result != null && result.status() == JobStatus.RUNNING)
                .handleResultIf(Objects::isNull)
                .withBackoff(500, 5000, ChronoUnit.MICROS)
                .withMaxRetries(60)
                .onFailedAttempt(event -> LOG.warn(
                        "Job {} status check attempt {}/61: {}",
                        jobId,
                        event.getAttemptCount(),
                        event.getLastException() != null
                                ? event.getLastException().getMessage()
                                : "still running"))
                .build();

        try {
            return Failsafe.with(retryPolicy).get(() -> {
                var results = client.getJobResults(jobId);
                if (results != null
                        && (results.status() == JobStatus.COMPLETED || results.status() == JobStatus.FAILED)) {
                    if (results.status() == JobStatus.FAILED) {
                        LOG.error("Job {} failed", jobId);
                    }
                    return results;
                }
                return results; // Will trigger retry if null or RUNNING
            });
        } catch (Exception e) {
            LOG.error("Job {} timed out or failed after 60 seconds", jobId);
            return null;
        }
    }
}
