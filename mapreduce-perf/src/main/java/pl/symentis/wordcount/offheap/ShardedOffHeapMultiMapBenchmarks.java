package pl.symentis.wordcount.offheap;

import static org.openjdk.jmh.annotations.Scope.Benchmark;

import java.io.IOException;
import java.nio.file.Files;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import pl.symentis.mapreduce.offheap.JavaSerializationStrategy;
import pl.symentis.mapreduce.offheap.ShardedOffHeapMultiMap;

@State(Benchmark)
public class ShardedOffHeapMultiMapBenchmarks {

    private ShardedOffHeapMultiMap<String, Long> stringLongShardedOffHeapMultiMap;

    @Setup
    public void setup() throws IOException {
        stringLongShardedOffHeapMultiMap = new ShardedOffHeapMultiMap<>(
                Files.createTempDirectory("offheap-benchmarks-"),
                new JavaSerializationStrategy<>(),
                new JavaSerializationStrategy<>());
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void append() throws IOException {
        for (int i = 0; i < 1000000; i++) {
            stringLongShardedOffHeapMultiMap.append("a", 1L);
        }
    }
}
