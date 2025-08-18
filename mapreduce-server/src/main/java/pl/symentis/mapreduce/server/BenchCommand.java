package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
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

        while (true) {
            try {
                String jobId = UUID.randomUUID().toString();
                var jarFile =
                        Paths.get("../mapreduce-wordcount-bundle/target/mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar");
                var dataFile = Paths.get("../mapreduce-wordcount/src/test/resources/big.txt");
                var jobParameters = Map.of("filename", "big.txt");

                client.submitJob(
                        jobId, jarFile, dataFile, "mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar", jobParameters);

                Thread.sleep(jobIntervalMillis);
            } catch (IOException | InterruptedException | MapReduceServerException e) {
                LOG.error("failed to submit new job", e);
            }
        }
    }
}
