package pl.symentis.wordcount.forkjoin;

import java.util.HashMap;
import org.openjdk.jmh.annotations.*;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.forkjoin.ForkJoinMapReduce;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class ForkJoinMapReduceWordCountBenchmark {

    @Param({"pl.symentis.wordcount.stopwords.ICUThreadLocalStopwords"})
    public String stopwordsClass;

    private WordCount wordCount;
    private MapReduce mapReduce;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        wordCount = new WordCount.Builder()
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new ForkJoinMapReduce.Builder().build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() {
        HashMap<String, Long> map = new HashMap<String, Long>();
        mapReduce.run(
                wordCount.input(ForkJoinMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
