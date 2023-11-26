package pl.symentis.mapreduce.workstealing;

import pl.symentis.mapreduce.core.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.stream.Collectors.*;

public class WorkStealingMapReduce implements MapReduce {

    public static class Builder {

        private int parallelism = Runtime.getRuntime().availableProcessors();
        private int phaserMaxTasks = 1000;
        private int batchSize = 10000;

        public Builder withParallelism(int parallelism) {
            this.parallelism = parallelism;
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

        public MapReduce build() {
            return new WorkStealingMapReduce(parallelism, phaserMaxTasks, batchSize);
        }
    }

    private final ExecutorService executorService;
    private final int phaserMaxTasks;
    private final int batchSize;

    public WorkStealingMapReduce(int parallelism, int phaserMaxTasks, int batchSize) {
        this.executorService = Executors.newWorkStealingPool(parallelism);
        this.phaserMaxTasks = phaserMaxTasks;
        this.batchSize = batchSize;
    }

    @Override
    public <In, MK, MV, RK, RV> void run(
            Input<In> input, Mapper<In, MK, MV> mapper, Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output) {

        Phaser rootPhaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                return phase == 0 && registeredParties == 0 && !input.hasNext();
            }
        };

        // map
        int tasksPerPhaser = 0;
        Phaser phaser = new Phaser(rootPhaser);

        ConcurrentLinkedDeque<Map<MK, List<MV>>> mapResults = new ConcurrentLinkedDeque<>();
        ArrayList<In> batch = new ArrayList<>(batchSize);

        while (input.hasNext()) {
            batch.add(input.next());

            if (batch.size() == batchSize || !input.hasNext()) {
                phaser.register();

                executorService.submit(
                        new MapperPhase<>(new IteratorInput<>(batch.iterator()), mapper, mapResults, phaser));

                tasksPerPhaser++;
                if (tasksPerPhaser >= phaserMaxTasks) {
                    phaser = new Phaser(rootPhaser);
                    tasksPerPhaser = 0;
                }
                batch = new ArrayList<>(batchSize);
            }
        }

        rootPhaser.awaitAdvance(0);

        // merge map results
        Map<MK, List<MV>> map = merge(mapResults, reducer);

        // reduce
        reduce(reducer, output, map);
    }

    private <MK, MV, RK, RV> void reduce(
            Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output, Map<MK, List<MV>> map) {
        Set<MK> keys = map.keySet();
        for (MK key : keys) {
            reducer.reduce(key, map.get(key), output);
        }
    }

    static <MK, MV, RK, RV> Map<MK, List<MV>> merge(
            ConcurrentLinkedDeque<Map<MK, List<MV>>> mapResults, Reducer<MK, MV, RK, RV> reducer) {
        return mapResults.parallelStream()
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(groupingBy(
                        Map.Entry::getKey,
                        mapping(
                                entry -> {
                                    HashMapOutput<RK, RV> out = new HashMapOutput<>();
                                    reducer.reduce(entry.getKey(), entry.getValue(), out);
                                    return entry.getValue();
                                },
                                reducing(new ArrayList<>(), WorkStealingMapReduce::sum))));
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

        private final Input<I> input;
        private final Mapper<I, K, V> mapper;
        private final Phaser phaser;
        private final Queue<Map<K, List<V>>> mapResults;

        MapperPhase(Input<I> input, Mapper<I, K, V> mapper, Queue<Map<K, List<V>>> mapResults, Phaser phaser) {
            this.input = input;
            this.mapper = mapper;
            this.mapResults = mapResults;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            HashMapOutput<K, V> output = new HashMapOutput<>();
            while (input.hasNext()) {
                mapper.map(input.next(), output);
            }
            mapResults.offer(output.asMap());
            phaser.arriveAndDeregister();
        }
    }

    private static <V> List<V> sum(List<V> op1, List<V> op2) {
        ArrayList<V> vs = new ArrayList<>(op1.size() + op2.size());
        vs.addAll(op1);
        vs.addAll(op2);
        return vs;
    }
}
