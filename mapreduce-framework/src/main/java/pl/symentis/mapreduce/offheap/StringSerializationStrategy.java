package pl.symentis.mapreduce.offheap;

public class StringSerializationStrategy implements SerializationStrategy<String> {

    @Override
    public byte[] serialize(String object) {
        return object.getBytes();
    }

    @Override
    public String deserialize(byte[] data) {
        return new String(data);
    }
}
