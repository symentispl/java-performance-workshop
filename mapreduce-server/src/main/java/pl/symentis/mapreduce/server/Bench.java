package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Directory;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "bench")
public class Bench implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Bench.class);
    private static final String PROCESS_NAME = "bench";

    @Option(name = "--jobs-dir")
    @Directory
    @Required
    private File jobsDir;

    @Option(name = "--job-interval-ms")
    private int jobIntervalMillis = 200;

    @AirlineModule
    private GlobalOptions globalOptions;

    @Override
    public void run() {

        Observer.getInstance()
                .setupRegistry(PROCESS_NAME, globalOptions.configFile)
                .setupObservationRegistry()
                .turnOnJvmMetrics();

        PyroscopeAgent.start(new Config.Builder()
                .setApplicationName(PROCESS_NAME)
                .setProfilingEvent(EventType.ITIMER)
                .setFormat(Format.JFR)
                .setServerAddress("http://localhost:4040")
                .build());

        var jobDefinition = new JobDefinition(
                "../mapreduce-wordcount-bundle/target/mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar",
                Map.of("filename", "../mapreduce-wordcount/src/test/resources/big.txt"));
        var gson = new Gson();

        while (true) {
            try {
                var tempFile = jobsDir.toPath().resolve(String.format("job-%s.json", UUID.randomUUID()));
                try (var writer =
                        Files.newBufferedWriter(tempFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                    gson.toJson(jobDefinition, writer);
                }
                Thread.sleep(jobIntervalMillis);
            } catch (IOException | InterruptedException e) {
                LOG.error("failed to submit new job", e);
            }
        }
    }
}
