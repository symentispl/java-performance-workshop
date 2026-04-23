package pl.symentis.wordcount.core;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import pl.symentis.mapreduce.batching.BatchingMapReduce;
import pl.symentis.mapreduce.core.Bootstrap;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.offheap.LongSerializationStrategy;
import pl.symentis.mapreduce.offheap.StringSerializationStrategy;
import pl.symentis.mapreduce.rocksdb.RocksDBMapperOutput;

public class RocksDBBatchingMapReduceWordCountTest implements WordCountMapReduceTest {

    @TempDir
    Path tempDir;

    @Override
    public MapReduce mapReduce(Bootstrap bootstrap) {
        return new BatchingMapReduce.Builder()
                .withMapperOutputSupplier(() -> new RocksDBMapperOutput<>(
                        tempDir, new StringSerializationStrategy(), new LongSerializationStrategy()))
                .build();
    }
}
