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
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.MapperOutput;
import pl.symentis.mapreduce.core.SequentialMapReduce;
import pl.symentis.wordcount.core.Stopwords;
import pl.symentis.wordcount.core.WordCount;

@State(Scope.Benchmark)
public class SequentialMapReduceWordCountBenchmark {

    @Param({"pl.symentis.mapreduce.core.HashMapOutput"})
    public String mapperOutputClass;

    @Param({"NonCollatorStopwords"})
    public String stopwordsClass;

    @Param({"StringTokenizerSplitter"})
    public String stringSplitterClass;

    private WordCount wordCount;
    private MapReduce mapReduce;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        wordCount = new WordCount.Builder(Bootstrap.create())
                .withStopwords((Class<? extends Stopwords>) Class.forName(stopwordsClass))
                .build();
        mapReduce = new SequentialMapReduce.Builder()
                .withMapperOutputSupplier(() -> {
                    try {
                        return (MapperOutput) Class.forName(mapperOutputClass).newInstance();
                    } catch (InstantiationException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
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
                wordCount.input(SequentialMapReduceWordCountBenchmark.class.getResourceAsStream("/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                map::put);
        return map;
    }
}
