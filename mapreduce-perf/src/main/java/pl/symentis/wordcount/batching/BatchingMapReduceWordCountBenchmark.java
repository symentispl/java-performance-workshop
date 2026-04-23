package pl.symentis.wordcount.batching;

import java.util.HashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class BatchingMapReduceWordCountBenchmark {

    @Param({"NonCollatorStopwords"})
    public String stopwordsClass;

    @Param({"StringTokenizerSplitter"})
    public String stringSplitterClass;

    @Param({"8"})
    public int threadPoolMaxSize;

    @Param({"1000"})
    public int phaserMaxTasks;

    @Param({"10000"})
    public int batchSize;

    private WordCount wordCount;
    private MapReduce mapReduce;
    private Bootstrap bootstrap;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        bootstrap = Bootstrap.create();
        wordCount = new WordCount.Builder(bootstrap)
                .withStopwords(stopwordsClass)
                .withStringSplitter(stringSplitterClass)
                .build();
        mapReduce = new BatchingMapReduce.Builder()
                .withPhaserMaxTasks(phaserMaxTasks)
                .withThreadPoolSize(threadPoolMaxSize)
                .withBatchSize(batchSize)
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        bootstrap.close();
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() throws Exception {
        HashMap<String, Long> map = new HashMap<>();
        mapReduce.run(
                wordCount.input(BatchingMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
