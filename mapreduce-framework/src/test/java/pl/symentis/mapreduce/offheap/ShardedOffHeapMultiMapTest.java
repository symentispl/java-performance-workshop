package pl.symentis.mapreduce.offheap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedOffHeapMultiMapTest {
    @Test
    void readWriteSingleKeySingleValue(@TempDir Path tempDir) throws IOException, ClassNotFoundException {
        try (var multiMap =
                new ShardedOffHeapMultiMap(tempDir, new JavaSerializationStrategy(), new JavaSerializationStrategy())) {
            multiMap.append("a", 1);
            var valueIterator = multiMap.getValues("a");
            assertThat(valueIterator).toIterable().containsExactly(1);
        }
    }

    @Test
    void readWriteSingleKeyMultipleValues(@TempDir Path tempDir) throws IOException, ClassNotFoundException {
        try (var multiMap =
                new ShardedOffHeapMultiMap(tempDir, new JavaSerializationStrategy(), new JavaSerializationStrategy())) {
            multiMap.append("a", 1);
            multiMap.append("a", 2);
            multiMap.append("a", 3);
            multiMap.append("a", 4);
            multiMap.append("a", 5);
            var valueIterator = multiMap.getValues("a");
            assertThat(valueIterator).toIterable().containsExactly(5,4,3,2,1);
        }
    }
}
