package pl.symentis.wordcount.sequential;

import org.openjdk.jmh.annotations.*;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.MapperOutput;
import pl.symentis.mapreduce.core.SequentialMapReduce;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

import java.util.HashMap;

@State(Scope.Benchmark)
public class SequentialMapReduceWordCountBenchmark {

    @Param({"pl.symentis.mapreduce.core.HashMapOutput"})
    public String mapperOutputClass;

    @Param({"pl.symentis.wordcount.core.NonThreadLocalStopwords"})
    public String stopwordsClass;

    private WordCount wordCount;
    private MapReduce mapReduce;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        wordCount = new WordCount
                .Builder()
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new SequentialMapReduce
                .Builder()
                .withMapperOutput((Class<? extends MapperOutput<?, ?>>) Class.forName(mapperOutputClass))
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        mapReduce.shutdown();
    }

    @Benchmark
    public Object countWords() {
        HashMap<String, Long> map = new HashMap<String, Long>();
        mapReduce.run(
                wordCount.input(SequentialMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(), map::put);
        return map;
    }

}
