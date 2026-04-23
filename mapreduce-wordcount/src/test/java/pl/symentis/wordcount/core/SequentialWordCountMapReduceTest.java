package pl.symentis.wordcount.core;

import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.SequentialMapReduce;

public class SequentialWordCountMapReduceTest implements WordCountMapReduceTest {
    @Override
    public MapReduce mapReduce(Bootstrap bootstrap) {
        return new SequentialMapReduce.Builder(bootstrap).build();
    }
}
