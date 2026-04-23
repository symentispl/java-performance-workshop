package pl.symentis.mapreduce.parallel;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import pl.symentis.mapreduce.core.Input;
import pl.symentis.mapreduce.core.MapReduce;
import pl.symentis.mapreduce.core.MapReduceException;
import pl.symentis.mapreduce.core.Mapper;
import pl.symentis.mapreduce.core.Output;
import pl.symentis.mapreduce.core.Reducer;

public class ParallelMapReduce implements MapReduce {

    public static class Builder {

        private int threadPoolMaxSize = Runtime.getRuntime().availableProcessors();
        private int phaserMaxTasks = 1000;

        public Builder withThreadPoolSize(int threadPoolMaxSize) {
            this.threadPoolMaxSize = threadPoolMaxSize;
            return this;
        }

        public Builder withPhaserMaxTasks(int phaserMaxTasks) {
            this.phaserMaxTasks = phaserMaxTasks;
            return this;
        }

        public MapReduce build() {
            return new ParallelMapReduce(threadPoolMaxSize, phaserMaxTasks);
        }
    }

    private final ExecutorService executorService;
    private final int phaserMaxTasks;

    public ParallelMapReduce(int threadPoolMaxSize, int phaserMaxTasks) {
        executorService = Executors.newFixedThreadPool(threadPoolMaxSize);
        this.phaserMaxTasks = phaserMaxTasks;
    }

    @Override
    public <In, MK, MV, RK, RV> void run(
            Input<In> input, Mapper<In, MK, MV> mapper, Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output) {

        Phaser rootPhaser = new Phaser(1) {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                return registeredParties == 0;
            }
        };

        // map
        Map<MK, Collection<MV>> map = new ConcurrentHashMap<>();
        int tasksPerPhaser = 0;
        Phaser phaser = new Phaser(rootPhaser);

        while (input.hasNext()) {
            In in = input.next();

            phaser.register();

            executorService.submit(new MapperPhase<>(in, mapper, map, phaser));

            tasksPerPhaser++;
            if (tasksPerPhaser >= phaserMaxTasks) {
                phaser = new Phaser(rootPhaser);
                tasksPerPhaser = 0;
            }
        }

        rootPhaser.arriveAndDeregister();
        rootPhaser.awaitAdvance(0);

        // reduce
        Set<MK> keys = map.keySet();
        for (MK key : keys) {
            reducer.reduce(key, map.get(key), output);
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

    static final class MapperPhase<I, K, V> implements Runnable {

        private final I in;
        private final Mapper<I, K, V> mapper;
        private final Map<K, Collection<V>> map;
        private final Phaser phaser;

        MapperPhase(I in, Mapper<I, K, V> mapper, Map<K, Collection<V>> map, Phaser phaser) {
            this.in = in;
            this.mapper = mapper;
            this.map = map;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            mapper.map(
                    in,
                    (k, v) -> map.compute(k, (key, oldValue) -> {
                        Collection<V> newValue = oldValue;
                        if (newValue == null) {
                            newValue = new ConcurrentLinkedQueue<>();
                        }
                        newValue.add(v);
                        return newValue;
                    }));
            phaser.arriveAndDeregister();
        }
    }
}
