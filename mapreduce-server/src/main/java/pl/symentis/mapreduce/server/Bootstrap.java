package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Directory;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.batching.BatchingParallelMapReduce;

@Command(name = "bootstrap")
public class Bootstrap implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    @Option(name = "--jobs-dir")
    @Directory(mustExist = true)
    @Required
    private File jobDir;

    private Server boot() throws IOException {
        return new Builder().jobsDir(jobDir.toPath()).build();
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
        private Path jobsDir;

        public Builder jobsDir(Path dir) {
            jobsDir = dir;
            return this;
        }

        public Server build() throws IOException {
            // will watch input dir for tasks (which will be jars)
            LOG.info("enabling watch service on directory {}", jobsDir.toAbsolutePath());
            var watchService = FileSystems.getDefault().newWatchService();
            var watchKey = jobsDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            LOG.info("initilizing map reduce framework");
            var mapReduce = new BatchingParallelMapReduce.Builder()
                    .withBatchSize(1000)
                    .withPhaserMaxTasks(10000)
                    .withThreadPoolSize(Runtime.getRuntime().availableProcessors())
                    .build();

            var executorService =
                    Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() + 1);
            var gson = new Gson();

            return new Server(jobsDir, watchService, watchKey, executorService, gson, mapReduce);
        }
    }
}
