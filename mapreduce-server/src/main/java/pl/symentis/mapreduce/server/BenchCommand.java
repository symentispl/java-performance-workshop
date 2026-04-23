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
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "bench")
public class BenchCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BenchCommand.class);
    private static final Logger LATENCY_STATS_LOG = LoggerFactory.getLogger("latencyStats");

    @Option(name = "--server-url")
    @Required
    private String serverUrl = "http://localhost:8080";

    @Option(name = "--job-interval-ms")
    private int jobIntervalMillis = 200;

    private final Recorder latencyRecorder = new Recorder(TimeUnit.HOURS.toMillis(1), 3);
    private final AtomicLong failedJobCount = new AtomicLong();

    @Override
    public void run() {
        var client = new MapReduceServerClient(serverUrl);
        var scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        var jobProcessor = Executors.newCachedThreadPool();

        LOG.debug("starting benchmark with job interval {}ms", jobIntervalMillis);

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

        scheduler.scheduleAtFixedRate(this::logLatencyStats, 10, 10, TimeUnit.SECONDS);

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
            logLatencyStats();
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOG.warn("benchmark interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private void logLatencyStats() {
        Histogram h = latencyRecorder.getIntervalHistogram();
        long failed = failedJobCount.getAndSet(0);
        if (h.getTotalCount() == 0 && failed == 0) {
            LATENCY_STATS_LOG.info("latency stats (last 10s): no completed jobs");
            return;
        }
        LATENCY_STATS_LOG.info(
                "latency stats (ms) count={} failed={} min={} p50={} p75={} p90={} p95={} p99={} p99.9={} max={} mean={}",
                h.getTotalCount(),
                failed,
                h.getMinValue(),
                h.getValueAtPercentile(50.0),
                h.getValueAtPercentile(75.0),
                h.getValueAtPercentile(90.0),
                h.getValueAtPercentile(95.0),
                h.getValueAtPercentile(99.0),
                h.getValueAtPercentile(99.9),
                h.getMaxValue(),
                String.format("%.1f", h.getMean()));
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
        var jarFile = Paths.get("../mapreduce-wordcount-bundle/target/mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar");
        var dataFile = Paths.get("../mapreduce-wordcount/src/test/resources/big.txt");
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
            latencyRecorder.recordValue(completionTime - startTime);
            if (results == null || results.status() == JobStatus.FAILED) {
                failedJobCount.incrementAndGet();
            }
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
                return results;
            });
        } catch (Exception e) {
            LOG.error("Job {} timed out or failed after 60 seconds", jobId);
            return null;
        }
    }
}
