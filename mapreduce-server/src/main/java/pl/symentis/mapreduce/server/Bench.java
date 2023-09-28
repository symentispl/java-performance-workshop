package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Directory;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
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

    @Option(name = "--jobs-dir")
    @Directory
    @Required
    private File jobsDir;

    @Option(name = "--job-interval-ms")
    private int jobIntervalMillis = 200;

    @Override
    public void run() {
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
