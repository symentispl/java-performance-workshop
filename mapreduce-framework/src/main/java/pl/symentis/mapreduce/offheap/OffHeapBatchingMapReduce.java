package pl.symentis.mapreduce.offheap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import pl.symentis.mapreduce.core.Input;
import pl.symentis.mapreduce.core.IteratorInput;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.MapReduceException;
import pl.symentis.mapreduce.core.Mapper;
import pl.symentis.mapreduce.core.Output;
import pl.symentis.mapreduce.core.Reducer;

public class OffHeapBatchingMapReduce implements MapReduce {

    public static class Builder {
        private int threadPoolMaxSize = Runtime.getRuntime().availableProcessors();
        private int phaserMaxTasks = 1000;
        private int batchSize = 10000;
        private SerializationStrategy<Serializable> keySerializer = new JavaSerializationStrategy<>();
        private SerializationStrategy<Serializable> valueSerializer = new JavaSerializationStrategy<>();

        public Builder withThreadPoolSize(int threadPoolMaxSize) {
            this.threadPoolMaxSize = threadPoolMaxSize;
            return this;
        }

        public Builder withPhaserMaxTasks(int phaserMaxTasks) {
            this.phaserMaxTasks = phaserMaxTasks;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <K extends Serializable> Builder withKeySerializer(SerializationStrategy<K> keySerializer) {
            this.keySerializer = (SerializationStrategy<Serializable>) keySerializer;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <V extends Serializable> Builder withValueSerializer(SerializationStrategy<V> valueSerializer) {
            this.valueSerializer = (SerializationStrategy<Serializable>) valueSerializer;
            return this;
        }

        public MapReduce build() {
            return new OffHeapBatchingMapReduce(
                    threadPoolMaxSize, phaserMaxTasks, batchSize, keySerializer, valueSerializer);
        }
    }

    private final ExecutorService executorService;
    private final int phaserMaxTasks;
    private final int batchSize;
    private final SerializationStrategy<Serializable> keySerializer;
    private final SerializationStrategy<Serializable> valueSerializer;

    public OffHeapBatchingMapReduce(
            int threadPoolMaxSize,
            int phaserMaxTasks,
            int batchSize,
            SerializationStrategy<Serializable> keySerializer,
            SerializationStrategy<Serializable> valueSerializer) {
        this.executorService = Executors.newFixedThreadPool(threadPoolMaxSize);
        this.phaserMaxTasks = phaserMaxTasks;
        this.batchSize = batchSize;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <In, MK, MV, RK, RV> void run(
            Input<In> input, Mapper<In, MK, MV> mapper, Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output) {

        Path tempDir;
        ShardedOffHeapMultiMap<MK, MV> offHeapMap;

        try {
            tempDir = Files.createTempDirectory("offheap-mapreduce-");
            offHeapMap = new ShardedOffHeapMultiMap<>(
                    tempDir, (SerializationStrategy<MK>) keySerializer, (SerializationStrategy<MV>) valueSerializer);
        } catch (IOException e) {
            throw new MapReduceException(e);
        }

        try {
            // Map phase - all mappers write to shared off-heap storage
            runMapPhase(input, mapper, offHeapMap);

            // Reduce phase - iterate over off-heap data
            runReducePhase(reducer, output, offHeapMap);

        } finally {
            offHeapMap.cleanup();
        }
    }

    private <In, MK, MV> void runMapPhase(
            Input<In> input, Mapper<In, MK, MV> mapper, ShardedOffHeapMultiMap<MK, MV> offHeapMap) {

        Phaser rootPhaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                return phase == 0 && registeredParties == 0 && !input.hasNext();
            }
        };

        int tasksPerPhaser = 0;
        Phaser phaser = new Phaser(rootPhaser);
        ArrayList<In> batch = new ArrayList<>(batchSize);

        while (input.hasNext()) {
            batch.add(input.next());

            if (batch.size() == batchSize || !input.hasNext()) {
                phaser.register();

                executorService.submit(
                        new OffHeapMapperPhase<>(new IteratorInput<>(batch.iterator()), mapper, offHeapMap, phaser));

                tasksPerPhaser++;
                if (tasksPerPhaser >= phaserMaxTasks) {
                    phaser = new Phaser(rootPhaser);
                    tasksPerPhaser = 0;
                }
                batch = new ArrayList<>(batchSize);
            }
        }

        rootPhaser.awaitAdvance(0);
    }

    private <MK, MV, RK, RV> void runReducePhase(
            Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output, ShardedOffHeapMultiMap<MK, MV> offHeapMap) {

        try {
            Iterator<MK> keyIterator = offHeapMap.getAllKeys();
            while (keyIterator.hasNext()) {
                MK key = keyIterator.next();
                ShardedOffHeapMultiMap.ValueIterator<MV> valueIterator = offHeapMap.getValues(key);

                // Convert iterator to list for reducer
                List<MV> values = new ArrayList<>();
                while (valueIterator.hasNext()) {
                    values.add(valueIterator.next());
                }

                reducer.reduce(key, values, output);
            }
        } catch (Exception e) {
            throw new MapReduceException(e);
        }
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new MapReduceException(e);
        }
    }

    static final class OffHeapMapperPhase<I, K, V> implements Runnable {
        private final Input<I> input;
        private final Mapper<I, K, V> mapper;
        private final ShardedOffHeapMultiMap<K, V> offHeapMap;
        private final Phaser phaser;

        OffHeapMapperPhase(
                Input<I> input, Mapper<I, K, V> mapper, ShardedOffHeapMultiMap<K, V> offHeapMap, Phaser phaser) {
            this.input = input;
            this.mapper = mapper;
            this.offHeapMap = offHeapMap;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            OffHeapOutput<K, V> output = new OffHeapOutput<>(offHeapMap);
            while (input.hasNext()) {
                mapper.map(input.next(), output);
            }
            phaser.arriveAndDeregister();
        }
    }

    private static class OffHeapOutput<K, V> implements Output<K, V> {
        private final ShardedOffHeapMultiMap<K, V> offHeapMap;

        OffHeapOutput(ShardedOffHeapMultiMap<K, V> offHeapMap) {
            this.offHeapMap = offHeapMap;
        }

        @Override
        public void emit(K key, V value) {
            try {
                offHeapMap.append(key, value);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to off-heap storage", e);
            }
        }
    }
}
