package pl.symentis.mapreduce.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ServerTest {

    private static Server server;
    private static MapReduceServerClient client;
    private static Path jobsDir;

    @BeforeAll
    static void setUp(@TempDir Path tempDir) throws IOException {
        jobsDir = tempDir;
        server = new Server.Builder().port(8080).jobsDir(jobsDir).build();
        server.start();
        client = new MapReduceServerClient("http://localhost:8080");
    }

    @AfterAll
    static void tearDown() throws Exception {
        try {
            client.close();
        } finally {
            server.stop();
        }
    }

    @Test
    void putJobContextFilesAndExecute() throws Exception {
        var jarFile = Paths.get("target/libs/mapreduce-wordcount.jar");
        var inputFile = Paths.get("target/libs/big.txt");
        var jobId = client.uploadJobFiles(jarFile, inputFile);
        assertThat(jobsDir.resolve(jobId + "/mapreduce-wordcount.jar")).hasSameBinaryContentAs(jarFile);
        assertThat(jobsDir.resolve(jobId + "/big.txt")).hasSameTextualContentAs(inputFile);
        var response = client.executeJob(jobId, "mapreduce-wordcount.jar", Map.of("filename", "big.txt"));
        assertThat(response).contains("Job accepted for processing");
    }

    @Test
    void submitJobWithInvalidJobId() throws Exception {
        assertThatThrownBy(() -> client.executeJob("invalid-job-id", "test.jar", Map.of("filename", "test.txt")))
                .hasMessageContaining("Job invalid-job-id not found");
    }

    @Test
    void submitJobGetResultsAndDelete() throws Exception {
        var jarFile = Paths.get("target/libs/mapreduce-wordcount.jar");
        var inputFile = Paths.get("target/libs/big.txt");
        var jobId =
                client.submitJob(jarFile, inputFile, jarFile.getFileName().toString(), Map.of("filename", "big.txt"));

        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    var results = client.getJobResults(jobId);
                    return results != null && results.status() == JobStatus.COMPLETED;
                });

        var results = client.getJobResults(jobId);
        assertThat(results).isNotNull();
        assertThat(results.status()).isEqualTo(JobStatus.COMPLETED);
        assertThat(results.results()).isNotEmpty();

        assertThat(jobsDir.resolve(jobId)).exists();

        var deletedResults = client.deleteJob(jobId);
        assertThat(deletedResults).isNotNull();
        assertThat(deletedResults.status()).isEqualTo(JobStatus.COMPLETED);

        assertThat(jobsDir.resolve(jobId)).doesNotExist();

        var nullResults = client.getJobResults(jobId);
        assertThat(nullResults).isNull();
    }
}
