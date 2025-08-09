package pl.symentis.wordcount.virtualthreads;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.virtualthreads.VirtualThreadsMapReduce;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

import java.util.HashMap;

@State(Scope.Benchmark)
public class VirtualThreadsMapReduceWordCountBenchmark {

    @Param({"pl.symentis.wordcount.stopwords.ICUThreadLocalStopwords"})
    public String stopwordsClass;

    @Param({"1000"})
    public int phaserMaxTasks;

    @Param({"10000"})
    public int batchSize;

    private WordCount wordCount;
    private MapReduce mapReduce;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        wordCount = new WordCount.Builder()
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new VirtualThreadsMapReduce.Builder()
                .withPhaserMaxTasks(phaserMaxTasks)
                .withBatchSize(batchSize)
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() {
        HashMap<String, Long> map = new HashMap<>();
        mapReduce.run(
                wordCount.input(VirtualThreadsMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
