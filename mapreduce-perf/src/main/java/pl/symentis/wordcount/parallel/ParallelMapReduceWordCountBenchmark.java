package pl.symentis.wordcount.parallel;

import java.util.HashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.parallel.ParallelMapReduce;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class ParallelMapReduceWordCountBenchmark {

    @Param({"NonCollatorStopwords"})
    public String stopwordsClass;

    @Param({"StringTokenizerSplitter"})
    public String stringSplitterClass;

    @Param({"8"})
    public int threadPoolMaxSize;

    @Param({"1000"})
    public int phaserMaxTasks;

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
        mapReduce = new ParallelMapReduce.Builder()
                .withPhaserMaxTasks(phaserMaxTasks)
                .withThreadPoolSize(threadPoolMaxSize)
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
                wordCount.input(ParallelMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
