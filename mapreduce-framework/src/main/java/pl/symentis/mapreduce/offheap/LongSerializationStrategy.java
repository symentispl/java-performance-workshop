package pl.symentis.mapreduce.offheap;

public class LongSerializationStrategy implements SerializationStrategy<Long> {

    @Override
    public byte[] serialize(Long object) {
        byte[] bytes = new byte[8];
        long value = object;
        bytes[0] = (byte) (value >>> 56);
        bytes[1] = (byte) (value >>> 48);
        bytes[2] = (byte) (value >>> 40);
        bytes[3] = (byte) (value >>> 32);
        bytes[4] = (byte) (value >>> 24);
        bytes[5] = (byte) (value >>> 16);
        bytes[6] = (byte) (value >>> 8);
        bytes[7] = (byte) (value);
        return bytes;
    }

    @Override
    public Long deserialize(byte[] data) {
        return ((long) (data[0] & 0xFF) << 56)
                | ((long) (data[1] & 0xFF) << 48)
                | ((long) (data[2] & 0xFF) << 40)
                | ((long) (data[3] & 0xFF) << 32)
                | ((long) (data[4] & 0xFF) << 24)
                | ((long) (data[5] & 0xFF) << 16)
                | ((long) (data[6] & 0xFF) << 8)
                | ((long) (data[7] & 0xFF));
    }
}
