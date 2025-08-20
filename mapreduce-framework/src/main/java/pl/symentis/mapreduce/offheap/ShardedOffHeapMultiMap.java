package pl.symentis.mapreduce.offheap;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ShardedOffHeapMultiMap<K, V> implements AutoCloseable {
    private static final int DEFAULT_SHARD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final long DEFAULT_INITIAL_SIZE = 64 * 1024 * 1024; // 64MB per shard

    private final OffHeapShard<K, V>[] shards;
    private final int shardCount;
    private final Function<K, Integer> hashFunction;
    private final Path baseDirectory;

    public ShardedOffHeapMultiMap(
            Path baseDirectory, SerializationStrategy<K> keySerializer, SerializationStrategy<V> valueSerializer)
            throws IOException {
        this(baseDirectory, keySerializer, valueSerializer, DEFAULT_SHARD_COUNT, DEFAULT_INITIAL_SIZE);
    }

    @SuppressWarnings("unchecked")
    public ShardedOffHeapMultiMap(
            Path baseDirectory,
            SerializationStrategy<K> keySerializer,
            SerializationStrategy<V> valueSerializer,
            int shardCount,
            long initialSizePerShard)
            throws IOException {
        this.shardCount = shardCount;
        this.hashFunction = key -> Math.abs(key.hashCode()) % shardCount;
        this.shards = new OffHeapShard[shardCount];

        this.baseDirectory = Files.createDirectories(baseDirectory);

        for (int i = 0; i < shardCount; i++) {
            Path dataPath = baseDirectory.resolve("shard_" + i + ".dat");
            Path keyIndexPath = baseDirectory.resolve("shard_" + i + ".keys");
            this.shards[i] =
                    new OffHeapShard<>(dataPath, keyIndexPath, keySerializer, valueSerializer, initialSizePerShard);
        }
    }

    public void append(K key, V value) throws IOException {
        int shardIndex = hashFunction.apply(key);
        shards[shardIndex].append(key, value);
    }

    public boolean containsKey(K key) throws IOException {
        int shardIndex = hashFunction.apply(key);
        return shards[shardIndex].containsKey(key);
    }

    public ValueIterator<V> getValues(K key) {
        int shardIndex = hashFunction.apply(key);
        return shards[shardIndex].getValues(key);
    }

    public Iterator<K> getAllKeys() {
        return new AllKeysIterator();
    }

    public void force() {
        for (OffHeapShard<K, V> shard : shards) {
            shard.force();
        }
    }

    public long totalSize() {
        long total = 0;
        for (OffHeapShard<K, V> shard : shards) {
            total += shard.size();
        }
        return total;
    }

    @Override
    public void close() throws IOException {
        force();
        IOException firstException = null;
        for (OffHeapShard<K, V> shard : shards) {
            try {
                shard.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public void cleanup() {
        try {
            close();
            try (var paths = Files.walk(baseDirectory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
            }
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    private class AllKeysIterator implements Iterator<K> {
        private int currentShardIndex = 0;
        private Iterator<K> currentShardIterator;

        public AllKeysIterator() {
            moveToNextNonEmptyShard();
        }

        @Override
        public boolean hasNext() {
            try {
                while (currentShardIndex < shardCount
                        && (currentShardIterator == null || !currentShardIterator.hasNext())) {
                    currentShardIndex++;
                    moveToNextNonEmptyShard();
                }
                return currentShardIterator != null && currentShardIterator.hasNext();
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return currentShardIterator.next();
        }

        private void moveToNextNonEmptyShard() {
            while (currentShardIndex < shardCount) {
                currentShardIterator = shards[currentShardIndex].getAllKeys();
                if (currentShardIterator.hasNext()) {
                    return;
                }
                currentShardIndex++;
            }
            currentShardIterator = null;
        }
    }

    private static class OffHeapShard<K, V> implements AutoCloseable {
        private static final int DATA_HEADER_SIZE = 24;
        private static final int KEY_INDEX_HEADER_SIZE = 24;
        private static final int KEY_HASH_SIZE = 4;
        private static final int KEY_LENGTH_SIZE = 4;
        private static final int VALUE_LENGTH_SIZE = 4;
        private static final int NEXT_POINTER_SIZE = 8;
        private static final int DATA_POSITION_SIZE = 8;
        private static final int DATA_ENTRY_HEADER_SIZE =
                KEY_HASH_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE + NEXT_POINTER_SIZE;
        private static final int KEY_INDEX_ENTRY_HEADER_SIZE = KEY_HASH_SIZE + KEY_LENGTH_SIZE + DATA_POSITION_SIZE;

        private final FileChannel dataChannel;
        private final FileChannel keyIndexChannel;
        private final SerializationStrategy<K> keySerializer;
        private final SerializationStrategy<V> valueSerializer;
        private final AtomicLong dataWritePosition = new AtomicLong(DATA_HEADER_SIZE);
        private final AtomicLong keyIndexWritePosition = new AtomicLong(KEY_INDEX_HEADER_SIZE);
        private final ConcurrentHashMap<Integer, Long> keyToDataPosition = new ConcurrentHashMap<>();
        private final Object writeLock = new Object();
        private volatile MappedByteBuffer dataBuffer;
        private volatile MappedByteBuffer keyIndexBuffer;
        private volatile long dataFileSize;
        private volatile long keyIndexFileSize;

        public OffHeapShard(
                Path dataFilePath,
                Path keyIndexFilePath,
                SerializationStrategy<K> keySerializer,
                SerializationStrategy<V> valueSerializer,
                long initialSize)
                throws IOException {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            this.dataChannel = FileChannel.open(
                    dataFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

            this.keyIndexChannel = FileChannel.open(
                    keyIndexFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

            initializeDataFile(initialSize);
            initializeKeyIndexFile(initialSize / 10); // Key index typically much smaller

            loadExistingData();
        }

        private void initializeDataFile(long initialSize) throws IOException {
            this.dataFileSize = Math.max(initialSize, dataChannel.size());
            if (dataChannel.size() < this.dataFileSize) {
                dataChannel.truncate(this.dataFileSize);
            }

            this.dataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.dataFileSize);

            if (dataChannel.size() == this.dataFileSize && dataBuffer.getLong(0) == 0) {
                initializeDataHeader();
            }
        }

        private void initializeKeyIndexFile(long initialSize) throws IOException {
            this.keyIndexFileSize = Math.max(initialSize, keyIndexChannel.size());
            if (keyIndexChannel.size() < this.keyIndexFileSize) {
                keyIndexChannel.truncate(this.keyIndexFileSize);
            }

            this.keyIndexBuffer = keyIndexChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.keyIndexFileSize);

            if (keyIndexChannel.size() == this.keyIndexFileSize && keyIndexBuffer.getLong(0) == 0) {
                initializeKeyIndexHeader();
            }
        }

        private void initializeDataHeader() {
            dataBuffer.putLong(0, System.currentTimeMillis());
            dataBuffer.putLong(8, 1L);
            dataBuffer.putLong(16, DATA_HEADER_SIZE);
        }

        private void initializeKeyIndexHeader() {
            keyIndexBuffer.putLong(0, System.currentTimeMillis());
            keyIndexBuffer.putLong(8, 1L);
            keyIndexBuffer.putLong(16, KEY_INDEX_HEADER_SIZE);
        }

        private void loadExistingData() {
            try {
                loadKeyIndex();
                long maxDataPosition = dataBuffer.getLong(16);
                dataWritePosition.set(maxDataPosition);
            } catch (Exception e) {
                dataWritePosition.set(DATA_HEADER_SIZE);
                keyIndexWritePosition.set(KEY_INDEX_HEADER_SIZE);
                keyToDataPosition.clear();
            }
        }

        private void loadKeyIndex() {
            try {
                long maxKeyIndexPosition = keyIndexBuffer.getLong(16);
                long position = KEY_INDEX_HEADER_SIZE;

                while (position < maxKeyIndexPosition) {
                    int keyHash = keyIndexBuffer.getInt((int) position);
                    int keyLength = keyIndexBuffer.getInt((int) position + KEY_HASH_SIZE);
                    long dataPosition = keyIndexBuffer.getLong((int) position + KEY_HASH_SIZE + KEY_LENGTH_SIZE);

                    keyToDataPosition.put(keyHash, dataPosition);
                    position += KEY_INDEX_ENTRY_HEADER_SIZE + keyLength;
                }
                keyIndexWritePosition.set(position);
            } catch (Exception e) {
                keyIndexWritePosition.set(KEY_INDEX_HEADER_SIZE);
                keyToDataPosition.clear();
            }
        }

        public void append(K key, V value) throws IOException {
            byte[] serializedKey = keySerializer.serialize(key);
            byte[] serializedValue = valueSerializer.serialize(value);

            int keyHash = key.hashCode();
            long dataEntrySize = DATA_ENTRY_HEADER_SIZE + serializedKey.length + serializedValue.length;

            synchronized (writeLock) {
                long currentDataPos = dataWritePosition.get();

                if (currentDataPos + dataEntrySize > dataFileSize) {
                    expandDataFile(currentDataPos + dataEntrySize);
                }

                Long existingDataPosition = keyToDataPosition.get(keyHash);
                long nextPointer = existingDataPosition != null ? existingDataPosition : 0L;

                writeDataEntry(currentDataPos, keyHash, serializedKey, serializedValue, nextPointer);

                if (existingDataPosition == null) {
                    addToKeyIndex(keyHash, serializedKey, currentDataPos);
                }

                keyToDataPosition.put(keyHash, currentDataPos);
                dataWritePosition.addAndGet(dataEntrySize);

                dataBuffer.putLong(16, dataWritePosition.get());
            }
        }

        private void addToKeyIndex(int keyHash, byte[] serializedKey, long dataPosition) throws IOException {
            long keyIndexEntrySize = KEY_INDEX_ENTRY_HEADER_SIZE + serializedKey.length;
            long currentKeyIndexPos = keyIndexWritePosition.get();

            if (currentKeyIndexPos + keyIndexEntrySize > keyIndexFileSize) {
                expandKeyIndexFile(currentKeyIndexPos + keyIndexEntrySize);
            }

            writeKeyIndexEntry(currentKeyIndexPos, keyHash, serializedKey, dataPosition);
            keyIndexWritePosition.addAndGet(keyIndexEntrySize);
            keyIndexBuffer.putLong(16, keyIndexWritePosition.get());
        }

        private void expandDataFile(long newSize) throws IOException {
            long expandedSize = Math.max(newSize, dataFileSize * 2);
            dataChannel.truncate(expandedSize);
            dataBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, expandedSize);
            dataFileSize = expandedSize;
        }

        private void expandKeyIndexFile(long newSize) throws IOException {
            long expandedSize = Math.max(newSize, keyIndexFileSize * 2);
            keyIndexChannel.truncate(expandedSize);
            keyIndexBuffer = keyIndexChannel.map(FileChannel.MapMode.READ_WRITE, 0, expandedSize);
            keyIndexFileSize = expandedSize;
        }

        private void writeDataEntry(long position, int keyHash, byte[] key, byte[] value, long nextPointer) {
            int pos = (int) position;

            dataBuffer.putInt(pos, keyHash);
            pos += KEY_HASH_SIZE;

            dataBuffer.putInt(pos, key.length);
            pos += KEY_LENGTH_SIZE;

            dataBuffer.putInt(pos, value.length);
            pos += VALUE_LENGTH_SIZE;

            dataBuffer.putLong(pos, nextPointer);
            pos += NEXT_POINTER_SIZE;

            dataBuffer.position(pos);
            dataBuffer.put(key);
            dataBuffer.put(value);
        }

        private void writeKeyIndexEntry(long position, int keyHash, byte[] key, long dataPosition) {
            int pos = (int) position;

            keyIndexBuffer.putInt(pos, keyHash);
            pos += KEY_HASH_SIZE;

            keyIndexBuffer.putInt(pos, key.length);
            pos += KEY_LENGTH_SIZE;

            keyIndexBuffer.putLong(pos, dataPosition);
            pos += DATA_POSITION_SIZE;

            keyIndexBuffer.position(pos);
            keyIndexBuffer.put(key);
        }

        public boolean containsKey(K key) throws IOException {
            int keyHash = key.hashCode();
            Long position = keyToDataPosition.get(keyHash);

            if (position == null) {
                return false;
            }

            return findMatchingEntry(key, keyHash, position) != -1;
        }

        public ValueIterator<V> getValues(K key) {
            int keyHash = key.hashCode();
            Long position = keyToDataPosition.get(keyHash);

            if (position == null) {
                return ValueIterator.empty();
            }

            return new ValueIterator<>(key, keyHash, position, keySerializer, valueSerializer, dataBuffer);
        }

        public Iterator<K> getAllKeys() {
            return new ShardKeysIterator();
        }

        private long findMatchingEntry(K targetKey, int targetKeyHash, long startPosition) throws IOException {
            byte[] targetKeyBytes = keySerializer.serialize(targetKey);
            long currentPos = startPosition;

            while (currentPos != 0) {
                int keyHash = dataBuffer.getInt((int) currentPos);
                if (keyHash != targetKeyHash) {
                    currentPos =
                            dataBuffer.getLong((int) currentPos + KEY_HASH_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE);
                    continue;
                }

                int keyLength = dataBuffer.getInt((int) currentPos + KEY_HASH_SIZE);
                if (keyLength != targetKeyBytes.length) {
                    currentPos =
                            dataBuffer.getLong((int) currentPos + KEY_HASH_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE);
                    continue;
                }

                byte[] storedKey = new byte[keyLength];
                dataBuffer.position((int) currentPos + DATA_ENTRY_HEADER_SIZE);
                dataBuffer.get(storedKey);

                if (java.util.Arrays.equals(targetKeyBytes, storedKey)) {
                    return currentPos;
                }

                currentPos = dataBuffer.getLong((int) currentPos + KEY_HASH_SIZE + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE);
            }

            return -1;
        }

        private class ShardKeysIterator implements Iterator<K> {
            private long currentPosition = KEY_INDEX_HEADER_SIZE;
            private final long maxPosition = keyIndexWritePosition.get();
            private K nextKey;
            private boolean hasNextComputed = false;

            @Override
            public boolean hasNext() {
                if (!hasNextComputed) {
                    try {
                        computeNext();
                    } catch (Exception e) {
                        nextKey = null;
                    }
                    hasNextComputed = true;
                }
                return nextKey != null;
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }

                K result = nextKey;
                nextKey = null;
                hasNextComputed = false;
                return result;
            }

            private void computeNext() throws IOException, ClassNotFoundException {
                if (currentPosition >= maxPosition) {
                    nextKey = null;
                    return;
                }

                int keyHash = keyIndexBuffer.getInt((int) currentPosition);
                int keyLength = keyIndexBuffer.getInt((int) currentPosition + KEY_HASH_SIZE);
                long dataPosition = keyIndexBuffer.getLong((int) currentPosition + KEY_HASH_SIZE + KEY_LENGTH_SIZE);

                byte[] keyBytes = new byte[keyLength];
                keyIndexBuffer.position((int) currentPosition + KEY_INDEX_ENTRY_HEADER_SIZE);
                keyIndexBuffer.get(keyBytes);

                nextKey = keySerializer.deserialize(keyBytes);
                currentPosition += KEY_INDEX_ENTRY_HEADER_SIZE + keyLength;
            }
        }

        public void force() {
            if (dataBuffer != null) {
                dataBuffer.force();
            }
            if (keyIndexBuffer != null) {
                keyIndexBuffer.force();
            }
        }

        public long size() {
            return keyToDataPosition.size();
        }

        @Override
        public void close() throws IOException {
            if (dataBuffer != null) {
                dataBuffer.force();
            }
            if (keyIndexBuffer != null) {
                keyIndexBuffer.force();
            }
            if (dataChannel != null) {
                dataChannel.close();
            }
            if (keyIndexChannel != null) {
                keyIndexChannel.close();
            }
        }
    }

    public static class ValueIterator<V> implements Iterator<V> {
        private static final ValueIterator<?> EMPTY = new ValueIterator<>(null, 0, 0L, null, null, null) {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                throw new java.util.NoSuchElementException();
            }
        };

        private final Object targetKey;
        private final int targetKeyHash;
        private final SerializationStrategy<Object> keySerializer;
        private final SerializationStrategy<V> valueSerializer;
        private final MappedByteBuffer buffer;
        private long currentPosition;
        private V nextValue;
        private boolean hasNextComputed = false;

        @SuppressWarnings("unchecked")
        public static <V> ValueIterator<V> empty() {
            return (ValueIterator<V>) EMPTY;
        }

        @SuppressWarnings("unchecked")
        private ValueIterator(
                Object key,
                int keyHash,
                long startPosition,
                SerializationStrategy<?> keySerializer,
                SerializationStrategy<V> valueSerializer,
                MappedByteBuffer buffer) {
            this.targetKey = key;
            this.targetKeyHash = keyHash;
            this.keySerializer = (SerializationStrategy<Object>) keySerializer;
            this.valueSerializer = valueSerializer;
            this.buffer = buffer;
            this.currentPosition = startPosition;
        }

        @Override
        public boolean hasNext() {
            if (!hasNextComputed) {
                try {
                    computeNext();
                } catch (Exception e) {
                    nextValue = null;
                }
                hasNextComputed = true;
            }
            return nextValue != null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }

            V result = nextValue;
            nextValue = null;
            hasNextComputed = false;
            return result;
        }

        private void computeNext() throws IOException, ClassNotFoundException {
            if (buffer == null || targetKey == null) {
                nextValue = null;
                return;
            }

            byte[] targetKeyBytes = keySerializer.serialize(targetKey);

            while (currentPosition != 0) {
                int keyHash = buffer.getInt((int) currentPosition);
                int keyLength = buffer.getInt((int) currentPosition + 4);
                int valueLength = buffer.getInt((int) currentPosition + 8);
                long nextPointer = buffer.getLong((int) currentPosition + 12);

                if (keyHash == targetKeyHash && keyLength == targetKeyBytes.length) {
                    byte[] storedKey = new byte[keyLength];
                    buffer.position((int) currentPosition + 20);
                    buffer.get(storedKey);

                    if (java.util.Arrays.equals(targetKeyBytes, storedKey)) {
                        byte[] valueBytes = new byte[valueLength];
                        buffer.get(valueBytes);
                        nextValue = valueSerializer.deserialize(valueBytes);
                        currentPosition = nextPointer;
                        return;
                    }
                }

                currentPosition = nextPointer;
            }

            nextValue = null;
        }
    }
}
