package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.offheap.LongSerializationStrategy;
import pl.symentis.mapreduce.offheap.StringSerializationStrategy;
import pl.symentis.mapreduce.rocksdb.RocksDBMapperOutput;

@Command(name = "bootstrap")
public class ServerCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerCommand.class);

    @Option(name = "--port")
    private int port = 8080;

    @Option(name = "--jobs-dir")
    @Required
    private Path jobsDir;

    @Option(name = "--mapper-output")
    private String mapperOutput = "hashmap";

    @Override
    public void run() {
        try {
            var server = new Builder()
                    .port(port)
                    .jobsDir(jobsDir)
                    .mapperOutput(mapperOutput)
                    .build();
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
        private String mapperOutput = "hashmap";

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder jobsDir(Path jobsDir) {
            this.jobsDir = jobsDir;
            return this;
        }

        public Builder mapperOutput(String mapperOutput) {
            this.mapperOutput = mapperOutput;
            return this;
        }

        public Server build() throws IOException {
            LOG.info("initializing map reduce framework with mapper output: {}", mapperOutput);

            var batchingBuilder = new BatchingMapReduce.Builder()
                    .withBatchSize(1000)
                    .withPhaserMaxTasks(10000)
                    .withThreadPoolSize(Runtime.getRuntime().availableProcessors());

            if ("rocksdb".equalsIgnoreCase(mapperOutput)) {
                var rocksDbPath = jobsDir.resolve("rocksdb");
                Files.createDirectories(rocksDbPath);
                var factory = new RocksDBMapperOutput.Factory(rocksDbPath);
                batchingBuilder.withMapperOutputSupplier(
                        () -> factory.create(new StringSerializationStrategy(), new LongSerializationStrategy()));
                var mapReduce = batchingBuilder.build();
                return Server.create(port, mapReduce, jobsDir, factory);
            }

            var mapReduce = batchingBuilder.build();
            return Server.create(port, mapReduce, jobsDir);
        }
    }
}
