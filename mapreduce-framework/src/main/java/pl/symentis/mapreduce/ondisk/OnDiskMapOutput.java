package pl.symentis.mapreduce.ondisk;

import org.apache.commons.io.input.CloseShieldInputStream;
import pl.symentis.mapreduce.core.MapperOutput;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyIterator;

/**
 * Naive implementation, which keeps a key values in bucket, which is append only file.
 *
 * @param <K>
 * @param <V>
 */
public class OnDiskMapOutput<K, V> implements MapperOutput<K, V> {

    private final LinkedBlockingQueue<Map.Entry<K, V>> queue;
    private final ExecutorService writer;
    private final Path baseDir;

    public OnDiskMapOutput() throws IOException {
        baseDir = Files.createTempDirectory("OnDiskMapOutput");
        queue = new LinkedBlockingQueue<>();
        writer = Executors.newSingleThreadExecutor();
        writer.submit(this::workOn);
    }

    private void workOn() {
        while (!writer.isShutdown()) {
            try {
                var newEntry = queue.poll(1, TimeUnit.SECONDS);
                if (newEntry != null) {
                    var uuidPath = keyFiles().computeIfAbsent(newEntry.getKey(), (k) -> Paths.get(UUID.randomUUID().toString()));
                    tryWriteKey(uuidPath, newEntry.getKey());
                    appendValue(uuidPath, newEntry.getValue());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void appendValue(Path uuidPath, V value) {
        var filename = uuidPath.toString() + ".values";
        var path = baseDir.resolve(filename);
        try (var os = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
             var oo = new ObjectOutputStream(os)) {
            oo.writeObject(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void tryWriteKey(Path uuidPath, K key) {
        var filename = uuidPath.toString() + ".keys";
        var path = baseDir.resolve(filename);
        if (!Files.exists(path)) {
            try (var os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                 var oo = new ObjectOutputStream(os)) {
                oo.writeObject(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void emit(K k, V v) {
        try {
            queue.put(Map.entry(k, v));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<K> keys() {
        return keyFiles().keySet();
    }

    private Map<K, Path> keyFiles() {
        try (var keyFiles = Files.newDirectoryStream(baseDir, "*.keys")) {
            var map = new HashMap<K, Path>();
            for (var keyFile : keyFiles) {
                try (var inputStream = Files.newInputStream(keyFile, StandardOpenOption.READ);
                     var oi = new ObjectInputStream(inputStream)) {
                    var key = (K) oi.readObject();
                    map.put(key, Path.of(withoutExtension(keyFile.getFileName())));
                }
            }
            return map;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<V> values(K k) {
        var keyFile = keyFiles().get(k);
        if (keyFile != null) {
            var valuesPath = baseDir.resolve(keyFile + ".values");
            var values = new ArrayList<V>();
            try (var is = Files.newInputStream(valuesPath, StandardOpenOption.READ)) {
                while (true) {
                    try (var oi = new ObjectInputStream(new CloseShieldInputStream(is))) {
                        var value = (V) oi.readObject();
                        values.add(value);
                    } catch (EOFException e) {
                        // end of file
                        break;
                    }
                }
                return values.iterator();
            } catch (IOException e) {
                e.printStackTrace();
                throw new UncheckedIOException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return emptyIterator();
    }

    public String withoutExtension(Path fileFullPath) {
        return fileFullPath.toString().substring(0, fileFullPath.toString().lastIndexOf('.'));
    }

    public void close() throws InterruptedException {
        writer.shutdown();
        writer.awaitTermination(1, TimeUnit.HOURS);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
