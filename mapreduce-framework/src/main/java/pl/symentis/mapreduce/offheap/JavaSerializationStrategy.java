package pl.symentis.mapreduce.offheap;

import java.io.*;

public class JavaSerializationStrategy<T extends Serializable> implements SerializationStrategy<T> {
    
    @Override
    public byte[] serialize(T object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            return bos.toByteArray();
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        }
    }
}