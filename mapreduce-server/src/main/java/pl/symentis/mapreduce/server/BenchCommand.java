package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
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
        var jobDefinition = new JobDefinition(
                "../mapreduce-wordcount-bundle/target/mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar",
                Map.of("filename", "../mapreduce-wordcount/src/test/resources/big.txt"));
        var gson = new Gson();
        
        var httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        while (true) {
            try {
                var requestBody = gson.toJson(jobDefinition);
                var request = HttpRequest.newBuilder()
                        .uri(URI.create(serverUrl + "/jobs"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .timeout(Duration.ofSeconds(30))
                        .build();

                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                LOG.info("Job submitted, response: {} {}", response.statusCode(), response.body());
                
                Thread.sleep(jobIntervalMillis);
            } catch (IOException | InterruptedException e) {
                LOG.error("failed to submit new job", e);
            }
        }
    }
}
