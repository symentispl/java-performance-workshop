package pl.symentis.mapreduce.rocksdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import pl.symentis.mapreduce.core.MapReduceException;
import pl.symentis.mapreduce.core.MapperOutput;
import pl.symentis.mapreduce.offheap.SerializationStrategy;

public class RocksDBMapperOutput<K, V> implements MapperOutput<K, V>, AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    static final int NAMESPACE_SIZE = 16;

    private final RocksDB db;
    private final Options options;
    private final byte[] namespace;
    private final byte[] namespaceEnd;
    private final SerializationStrategy<K> keySerializer;
    private final SerializationStrategy<V> valueSerializer;
    private final AtomicLong counter = new AtomicLong();

    public RocksDBMapperOutput(
            Path dbPath, SerializationStrategy<K> keySerializer, SerializationStrategy<V> valueSerializer) {
        try {
            this.options = new Options().setCreateIfMissing(true).useFixedLengthPrefixExtractor(NAMESPACE_SIZE);
            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        } catch (RocksDBException e) {
            throw new MapReduceException(e);
        }
        UUID uuid = UUID.randomUUID();
        this.namespace = uuidToBytes(uuid);
        this.namespaceEnd = incrementBytes(this.namespace);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    // shared-connection constructor — does not own the db/options lifecycle
    RocksDBMapperOutput(RocksDB db, SerializationStrategy<K> keySerializer, SerializationStrategy<V> valueSerializer) {
        this.db = db;
        this.options = null;
        UUID uuid = UUID.randomUUID();
        this.namespace = uuidToBytes(uuid);
        this.namespaceEnd = incrementBytes(this.namespace);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void close() {
        try {
            db.deleteRange(namespace, namespaceEnd);
        } catch (RocksDBException e) {
            throw new MapReduceException(e);
        }
        if (options != null) {
            db.close();
            options.close();
        }
    }

    /**
     * Opens a single RocksDB connection and vends {@link RocksDBMapperOutput} instances that share it,
     * each isolated by a unique UUID namespace. Close the factory when the owning component shuts down.
     */
    public static class Factory implements AutoCloseable {

        private final RocksDB db;
        private final Options options;

        public Factory(Path dbPath) {
            try {
                this.options = new Options().setCreateIfMissing(true).useFixedLengthPrefixExtractor(NAMESPACE_SIZE);
                this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
            } catch (RocksDBException e) {
                throw new MapReduceException(e);
            }
        }

        public <K, V> RocksDBMapperOutput<K, V> create(
                SerializationStrategy<K> keySerializer, SerializationStrategy<V> valueSerializer) {
            return new RocksDBMapperOutput<>(db, keySerializer, valueSerializer);
        }

        @Override
        public void close() {
            db.close();
            options.close();
        }
    }

    private static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return bytes;
    }

    @Override
    public void emit(K k, V v) {
        try {
            byte[] serializedK = keySerializer.serialize(k);
            byte[] serializedV = valueSerializer.serialize(v);
            long seq = counter.getAndIncrement();
            byte[] rocksKey = buildKey(serializedK, seq);
            db.put(rocksKey, serializedV);
        } catch (RocksDBException | IOException e) {
            throw new MapReduceException(e);
        }
    }

    @Override
    public Set<K> keys() {
        Set<K> keys = new HashSet<>();
        try (ReadOptions readOptions = new ReadOptions().setPrefixSameAsStart(true);
                RocksIterator iter = db.newIterator(readOptions)) {
            for (iter.seek(namespace); iter.isValid(); iter.next()) {
                byte[] rawKey = iter.key();
                if (rawKey.length < NAMESPACE_SIZE + 4) {
                    break;
                }
                if (!startsWith(rawKey, namespace)) {
                    break;
                }
                int keyLen = ByteBuffer.wrap(rawKey, NAMESPACE_SIZE, 4).getInt();
                if (rawKey.length < NAMESPACE_SIZE + 4 + keyLen + 8) {
                    break;
                }
                byte[] serializedK = Arrays.copyOfRange(rawKey, NAMESPACE_SIZE + 4, NAMESPACE_SIZE + 4 + keyLen);
                keys.add(keySerializer.deserialize(serializedK));
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new MapReduceException(e);
        }
        return keys;
    }

    @Override
    public Iterator<V> values(K k) {
        try {
            byte[] serializedK = keySerializer.serialize(k);
            byte[] keyPrefix = buildKeyPrefix(serializedK);
            byte[] upperBound = incrementBytes(keyPrefix);
            List<V> result = new ArrayList<>();
            try (ReadOptions readOptions =
                            new ReadOptions().setTotalOrderSeek(true).setIterateUpperBound(new Slice(upperBound));
                    RocksIterator iter = db.newIterator(readOptions)) {
                for (iter.seek(keyPrefix); iter.isValid(); iter.next()) {
                    result.add(valueSerializer.deserialize(iter.value()));
                }
            }
            return result.iterator();
        } catch (IOException | ClassNotFoundException e) {
            throw new MapReduceException(e);
        }
    }

    private byte[] buildKey(byte[] serializedK, long seq) {
        byte[] key = new byte[NAMESPACE_SIZE + 4 + serializedK.length + 8];
        ByteBuffer buf = ByteBuffer.wrap(key);
        buf.put(namespace);
        buf.putInt(serializedK.length);
        buf.put(serializedK);
        buf.putLong(seq);
        return key;
    }

    private byte[] buildKeyPrefix(byte[] serializedK) {
        byte[] prefix = new byte[NAMESPACE_SIZE + 4 + serializedK.length];
        ByteBuffer buf = ByteBuffer.wrap(prefix);
        buf.put(namespace);
        buf.putInt(serializedK.length);
        buf.put(serializedK);
        return prefix;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    static byte[] incrementBytes(byte[] input) {
        byte[] result = Arrays.copyOf(input, input.length);
        for (int i = result.length - 1; i >= 0; i--) {
            if ((++result[i]) != 0) {
                break;
            }
        }
        return result;
    }
}
