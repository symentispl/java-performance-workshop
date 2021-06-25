package pl.symentis.wordcount.core;

import org.junit.jupiter.api.Test;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.SequentialMapReduce;
import pl.symentis.wordcount.core.WordCount;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public interface WordCountMapReduceTest {

    MapReduce mapReduce();

    @Test
    default void mapReduceWordCount() throws FileNotFoundException {
        WordCount wordCount = new WordCount.Builder().build();

        MapReduce workflow = new SequentialMapReduce.Builder().build();
        Map<String, Long> smap = new HashMap<>();
        workflow.run(
                wordCount.input(new File("src/test/resources/big.txt")),
                wordCount.mapper(),
                wordCount.reducer(),
                smap::put);
        workflow.shutdown();

        workflow = mapReduce();
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
