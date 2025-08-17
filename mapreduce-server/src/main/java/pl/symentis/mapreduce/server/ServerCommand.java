package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.UncheckedIOException;
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

    private Server boot() throws IOException {
        return new Builder().port(port).build();
    }

    @Override
    public void run() {
        try {
            boot().start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder {
        private int port = 8080;

        public Builder port(int port) {
            this.port = port;
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

            return new Server(javalin, port, executorService, gson, mapReduce);
        }
    }
}
