package pl.symentis.mapreduce.offheap;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShardedOffHeapMultiMapTest {
    @Test
    void readWriteSingleKeySingleValue(@TempDir Path tempDir) throws IOException, ClassNotFoundException {
        // given
        try (var multiMap =
                new ShardedOffHeapMultiMap(tempDir, new JavaSerializationStrategy(), new JavaSerializationStrategy())) {
            // when
            multiMap.append("a", 1);
            // then
            assertThat(multiMap.getValues("a")).toIterable().containsExactly(1);
            // when
            var allKeys = multiMap.getAllKeys();
            // then
            assertThat(allKeys).toIterable().containsOnly("a");
        }
    }

    @Test
    void readWriteSingleKeyMultipleValues(@TempDir Path tempDir) throws Exception {
        // given
        try (var multiMap =
                new ShardedOffHeapMultiMap(tempDir, new JavaSerializationStrategy(), new JavaSerializationStrategy())) {
            // when
            multiMap.append("a", 1);
            multiMap.append("a", 2);
            multiMap.append("a", 3);
            multiMap.append("a", 4);
            multiMap.append("a", 5);
            // then
            assertThat(multiMap.getValues("a")).toIterable().containsExactly(5, 4, 3, 2, 1);
            // when
            var allKeys = multiMap.getAllKeys();
            // then
            assertThat(allKeys).toIterable().containsOnly("a");
        }
    }

    @Test
    void readWriteMultpileKeysSingleValues(@TempDir Path tempDir) throws Exception {
        // given
        try (var multiMap =
                new ShardedOffHeapMultiMap(tempDir, new JavaSerializationStrategy(), new JavaSerializationStrategy())) {
            // when
            multiMap.append("a", 1);
            multiMap.append("b", 2);
            multiMap.append("c", 3);
            multiMap.append("d", 4);
            multiMap.append("e", 5);
            // then
            assertThat(multiMap.getValues("a")).toIterable().containsExactly(1);
            assertThat(multiMap.getValues("b")).toIterable().containsExactly(2);
            assertThat(multiMap.getValues("c")).toIterable().containsExactly(3);
            assertThat(multiMap.getValues("d")).toIterable().containsExactly(4);
            assertThat(multiMap.getValues("e")).toIterable().containsExactly(5);
            // when
            var allKeys = multiMap.getAllKeys();
            // then
            assertThat(allKeys).toIterable().containsOnly("a", "b", "c", "d", "e");
        }
    }
}
