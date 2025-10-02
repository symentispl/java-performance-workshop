package pl.symentis.wordcount.sequential;

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
import pl.symentis.mapreduce.core.SequentialMapReduce;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class SequentialMapReduceWordCountBenchmark {

    @Param({"HashMapOutput"})
    public String mapperOutputClass;

    @Param({"NonCollatorStopwords"})
    public String stopwordsClass;

    @Param({"StringTokenizerSplitter"})
    public String stringSplitterClass;

    private WordCount wordCount;
    private MapReduce mapReduce;
    private Bootstrap bootstrap;

    @Setup(Level.Trial)
    public void setUp() {
        bootstrap = Bootstrap.create();
        wordCount = new WordCount.Builder(bootstrap)
                .withStopwords(stopwordsClass)
                .withStringSplitter(stringSplitterClass)
                .build();
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
        HashMap<String, Long> map = new HashMap<>();
        mapReduce.run(
                wordCount.input(SequentialMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
