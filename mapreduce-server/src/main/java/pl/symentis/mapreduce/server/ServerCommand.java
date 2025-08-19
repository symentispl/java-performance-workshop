package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "bootstrap")
public class ServerCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerCommand.class);

    @Option(name = "--port")
    private int port = 8080;

    @Option(name = "--jobs-dir")
    @Required
    private Path jobsDir;

    @Override
    public void run() {
        try {
            var server = new Server.Builder().port(port).jobsDir(jobsDir).build();
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
}
