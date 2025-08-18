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

    public boolean uploadJobFiles(String jobId, Path jarFile, Path dataFile)
            throws IOException, InterruptedException, MapReduceServerException {
        var multipartBody = MultipartBodyPublisher.newBuilder()
                .filePart(jarFile.getFileName().toString(), jarFile, MediaType.of("application", "java-archive"))
                .filePart(dataFile.getFileName().toString(), dataFile, MediaType.of("text", "plain"))
                .build();

        var request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/jobs/" + jobId))
                .PUT(multipartBody)
                .timeout(Duration.ofSeconds(30))
                .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.debug("files uploaded for job {}, response: {} {}", jobId, response.statusCode(), response.body());

        if (response.statusCode() != 200) {
            throw new MapReduceServerException(response.body());
        }
        return true;
    }

    public void executeJob(String jobId, String jarFileName, Map<String, String> jobParameters)
            throws IOException, InterruptedException {
        var jobDefinition = new JobDefinition(jarFileName, jobParameters);

        var requestBody = gson.toJson(jobDefinition);
        var request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/jobs/" + jobId))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(30))
                .build();

        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("Job {} executed, response: {} {}", jobId, response.statusCode(), response.body());
    }

    public boolean submitJob(
            String jobId, Path jarFile, Path dataFile, String jarFileName, Map<String, String> jobParameters)
            throws IOException, InterruptedException, MapReduceServerException {
        if (uploadJobFiles(jobId, jarFile, dataFile)) {
            executeJob(jobId, jarFileName, jobParameters);
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
