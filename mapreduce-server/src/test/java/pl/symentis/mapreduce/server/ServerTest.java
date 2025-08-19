package pl.symentis.mapreduce.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    void putJobContextFiles() throws Exception {
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
        try {
            client.executeJob("non-existent-job-id", "test.jar", Map.of("filename", "test.txt"));
        } catch (Exception e) {
            // Expected - this should fail with a JSON error response
            assertThat(e.getMessage()).contains("Job directory not found");
        }
    }
}
