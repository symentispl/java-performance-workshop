package pl.symentis.mapreduce.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        server = new ServerCommand.Builder().port(8080).jobsDir(tempDir).build();
        server.start();

        client = new MapReduceServerClient("http://localhost:8080");
        jobsDir = tempDir;
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
        client.uploadJobFiles(
                "first",
                Paths.get("target/libs/mapreduce-wordcount.jar"),
                Paths.get("target/libs/mapreduce-wordcount.jar"));
        assertThat(jobsDir.resolve("first/mapreduce-wordcount.jar")).isNotEmptyFile();
    }
}
