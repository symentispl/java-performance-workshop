package pl.symentis.wordcount.rocksdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.offheap.LongSerializationStrategy;
import pl.symentis.mapreduce.offheap.StringSerializationStrategy;
import pl.symentis.mapreduce.rocksdb.RocksDBMapperOutput;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class RocksDBMapReduceWordCountBenchmark {

    @Param({"pl.symentis.wordcount.stopwords.ICUThreadLocalStopwords"})
    public String stopwordsClass;

    @Param({"8"})
    public int threadPoolMaxSize;

    @Param({"1000"})
    public int phaserMaxTasks;

    @Param({"10000"})
    public int batchSize;

    private WordCount wordCount;
    private MapReduce mapReduce;
    private Path rocksDbPath;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        rocksDbPath = Files.createTempDirectory("rocksdb-bench-");
        wordCount = new WordCount.Builder()
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new BatchingMapReduce.Builder()
                .withPhaserMaxTasks(phaserMaxTasks)
                .withThreadPoolSize(threadPoolMaxSize)
                .withBatchSize(batchSize)
                .withMapperOutputSupplier(
                        () -> new RocksDBMapperOutput<>(rocksDbPath, new StringSerializationStrategy(), new LongSerializationStrategy()))
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        mapReduce.shutdown();
        Files.walk(rocksDbPath).sorted(Comparator.reverseOrder()).forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (IOException ignored) {
            }
        });
    }

    @Benchmark
    public Object countWords() throws Exception {
        HashMap<String, Long> map = new HashMap<>();
        mapReduce.run(
                wordCount.input(RocksDBMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
