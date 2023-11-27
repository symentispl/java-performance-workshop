package pl.symentis.wordcount.core;

import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.SequentialMapReduce;

public class SequentialWordCountMapReduceTest implements WordCountMapReduceTest{
    @Override
    public MapReduce mapReduce() {
        return new SequentialMapReduce.Builder().build();
    }
}
