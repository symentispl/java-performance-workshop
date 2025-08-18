package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.batching.BatchingMapReduce;

@Command(name = "bootstrap")
public class ServerCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerCommand.class);

    @Option(name = "--port")
    @Required
    private int port = 8080;

    @Override
    public void run() {
        try {
            var server = new Builder().port(port).build();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.stop();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder {
        private int port = 8080;
        private Path jobsDir;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder jobsDir(Path jobsDir) {
            this.jobsDir = jobsDir;
            return this;
        }

        public Server build() throws IOException {
            LOG.info("initializing HTTP server on port {}", port);
            var javalin = Javalin.create();

            LOG.info("initializing map reduce framework");
            var mapReduce = new BatchingMapReduce.Builder()
                    .withBatchSize(1000)
                    .withPhaserMaxTasks(10000)
                    .withThreadPoolSize(Runtime.getRuntime().availableProcessors())
                    .build();

            var executorService =
                    Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() + 1);
            var gson = new Gson();

            return new Server(javalin, port, executorService, gson, mapReduce, jobsDir);
        }
    }
}
