package pl.symentis.mapreduce.offheap;

import java.io.IOException;

public interface SerializationStrategy<T> {
    byte[] serialize(T object) throws IOException;
    T deserialize(byte[] data) throws IOException, ClassNotFoundException;
}