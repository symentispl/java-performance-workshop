package pl.symentis.wordcount.core;

import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;

public class BatchingWordCountMapReduceTest implements WordCountMapReduceTest {
    @Override
    public MapReduce mapReduce(Bootstrap bootstrap) {
        return new BatchingMapReduce.Builder().build();
    }
}
