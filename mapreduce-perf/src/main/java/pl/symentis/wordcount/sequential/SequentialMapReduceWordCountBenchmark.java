package pl.symentis.wordcount.sequential;

import java.util.HashMap;
import org.openjdk.jmh.annotations.*;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.SequentialMapReduce;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class SequentialMapReduceWordCountBenchmark {

    @Param({"HashMapOutput"})
    public String mapperOutputClass;

    @Param({"NonThreadLocalStopwords"})
    public String stopwordsClass;

    private WordCount wordCount;
    private MapReduce mapReduce;
    private Bootstrap bootstrap;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        bootstrap = Bootstrap.create();
        wordCount =
                new WordCount.Builder(bootstrap).withStopwords(stopwordsClass).build();
        mapReduce = new SequentialMapReduce.Builder(bootstrap)
                .withMapperOutput(mapperOutputClass)
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        bootstrap.close();
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() {
        HashMap<String, Long> map = new HashMap<String, Long>();
        mapReduce.run(
                wordCount.input(SequentialMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
