package pl.symentis.mapreduce.ondisk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

public class OnDiskMapOutputSupplier implements Supplier<OnDiskMapOutput> {

    private final Path baseDir;

    public OnDiskMapOutputSupplier() throws IOException {
        baseDir = Files.createTempDirectory("OnDiskMapOutput");
    }

    @Override
    public OnDiskMapOutput get() {
        return null;
    }
}
