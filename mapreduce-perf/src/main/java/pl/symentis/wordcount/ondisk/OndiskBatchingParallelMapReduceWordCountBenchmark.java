package pl.symentis.wordcount.ondisk;

import org.openjdk.jmh.annotations.*;
import pl.symentis.mapreduce.batching.BatchingParallelMapReduce;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.ondisk.OndiskBatchingParallelMapReduce;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

import java.io.File;
import java.util.HashMap;

@State(Scope.Benchmark)
public class OndiskBatchingParallelMapReduceWordCountBenchmark {

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

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        wordCount = new WordCount
                .Builder()
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new OndiskBatchingParallelMapReduce
                .Builder()
                .withPhaserMaxTasks(phaserMaxTasks)
                .withThreadPoolSize(threadPoolMaxSize)
                .withBatchSize(batchSize)
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() throws Exception {
        HashMap<String, Long> map = new HashMap<String, Long>();
        mapReduce.run(
                wordCount.input(new File("huge.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }

}
