package pl.symentis.mapreduce.server;

import com.github.mizosoft.methanol.MediaType;
import com.github.mizosoft.methanol.Methanol;
import com.github.mizosoft.methanol.MultipartBodyPublisher;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceServerClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MapReduceServerClient.class);

    private final String serverUrl;
    private final Methanol httpClient;
    private final Gson gson;

    public MapReduceServerClient(String serverUrl) {
        this.serverUrl = serverUrl;
        this.httpClient =
                Methanol.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.gson = new Gson();
    }

    public String uploadJobFiles(Path jarFile, Path dataFile)
            throws IOException, InterruptedException, MapReduceServerException {
        var multipartBody = MultipartBodyPublisher.newBuilder()
                .filePart(jarFile.getFileName().toString(), jarFile, MediaType.of("application", "java-archive"))
                .filePart(dataFile.getFileName().toString(), dataFile, MediaType.of("text", "plain"))
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/jobs/"))
                .PUT(multipartBody)
                .timeout(Duration.ofSeconds(30))
                .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.debug("files uploaded, response: {} {}", response.statusCode(), response.body());

        if (response.statusCode() != 201) {
            throw new MapReduceServerException(response.body());
        }

        var responseJson = gson.fromJson(response.body(), Map.class);
        return (String) responseJson.get("jobId");
    }

    public String executeJob(String jobId, String jarFileName, Map<String, String> jobParameters)
            throws IOException, InterruptedException {
        var jobDefinition = new SubmitJobRequest(jarFileName, jobParameters);

        var requestBody = gson.toJson(jobDefinition);
        var request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/jobs/" + jobId))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(30))
                .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("job {} executed, response: {} {}", jobId, response.statusCode(), response.body());
        var responseJson = gson.fromJson(response.body(), Map.class);
        return (String) responseJson.get("message");
    }

    public String submitJob(Path jarFile, Path dataFile, String jarFileName, Map<String, String> jobParameters)
            throws IOException, InterruptedException, MapReduceServerException {
        var jobId = uploadJobFiles(jarFile, dataFile);
        executeJob(jobId, jarFileName, jobParameters);
        return jobId;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
