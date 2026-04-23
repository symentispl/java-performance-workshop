package pl.symentis.mapreduce.offheap;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import pl.symentis.mapreduce.core.MapReduceException;
import pl.symentis.mapreduce.core.MapperOutput;

public class ShardedOffHeapMapperOutput<K, V> implements MapperOutput<K, V>, AutoCloseable {

    private final ShardedOffHeapMultiMap<K, V> offHeapMap;

    public ShardedOffHeapMapperOutput(
            SerializationStrategy<K> keySerializer, SerializationStrategy<V> valueSerializer) {
        try {
            offHeapMap = new ShardedOffHeapMultiMap<>(
                    Files.createTempDirectory("offheap-mapreduce-"), keySerializer, valueSerializer);
        } catch (IOException e) {
            throw new MapReduceException(e);
        }
    }

    @Override
    public void emit(K k, V v) {
        try {
            offHeapMap.append(k, v);
        } catch (IOException e) {
            throw new MapReduceException(e);
        }
    }

    @Override
    public Set<K> keys() {
        Set<K> result = new LinkedHashSet<>();
        Iterator<K> iter = offHeapMap.getAllKeys();
        while (iter.hasNext()) {
            result.add(iter.next());
        }
        return result;
    }

    @Override
    public Iterator<V> values(K k) {
        return offHeapMap.getValues(k);
    }

    @Override
    public void close() {
        offHeapMap.cleanup();
    }
}
