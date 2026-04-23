package pl.symentis.wordcount.core;

import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.parallel.ParallelMapReduce;

public class ParallelWordCountMapReduceTest implements WordCountMapReduceTest {
    @Override
    public MapReduce mapReduce(Bootstrap bootstrap) {
        return new ParallelMapReduce.Builder().build();
    }
}
