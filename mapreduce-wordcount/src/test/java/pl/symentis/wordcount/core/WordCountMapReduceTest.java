package pl.symentis.wordcount.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.SequentialMapReduce;

public interface WordCountMapReduceTest {

    MapReduce mapReduce(Bootstrap bootstrap);

    @Test
    default void mapReduceWordCount() throws FileNotFoundException {
        var bootstrap = Bootstrap.create();
        WordCount wordCount = new WordCount.Builder(bootstrap).build();

        MapReduce workflow = new SequentialMapReduce.Builder().build();
        Map<String, Long> smap = new HashMap<>();
        workflow.run(
                wordCount.input(new File("src/test/resources/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                smap::put);
        workflow.shutdown();

        workflow = mapReduce(bootstrap);
        Map<String, Long> fmap = new HashMap<>();
        workflow.run(
                wordCount.input(new File("src/test/resources/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                fmap::put);
        workflow.shutdown();

        assertThat(fmap).isEqualTo(smap);
    }
}
