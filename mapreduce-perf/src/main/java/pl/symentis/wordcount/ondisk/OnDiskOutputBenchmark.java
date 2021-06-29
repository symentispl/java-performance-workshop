package pl.symentis.wordcount.ondisk;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.Control;
import pl.symentis.mapreduce.ondisk.OnDiskMapOutput;

import java.io.IOException;

public class OnDiskOutputBenchmark {


    @State(Scope.Benchmark)
    public static class EmptyOnDiskMapOutput {

        @Param({"10000"})
        public int batchSize;

        private OnDiskMapOutput<String, Integer> onDiskMapOutput;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            onDiskMapOutput = new OnDiskMapOutput<>();
        }

    }

    @State(Scope.Benchmark)
    public static class FullOnDiskMapOutput {
        private OnDiskMapOutput<String, Integer> onDiskMapOutput;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            onDiskMapOutput = new OnDiskMapOutput<>();
            for (int i = 0; i < 1000; i++) {
                onDiskMapOutput.emit("a", 1);
            }
            while (!onDiskMapOutput.isEmpty()) {
                // wait until queue is drained
                Thread.onSpinWait();
            }
        }

        @TearDown
        public void tearDown() throws InterruptedException {
            onDiskMapOutput.close();
        }
    }

    @Benchmark
    @Group("emitAndFetch")
    @GroupThreads(8)
    @BenchmarkMode(Mode.SingleShotTime)
    public void emitKeyValue(EmptyOnDiskMapOutput state) {
        System.out.println("start to emit");
        for (int i = 0; i < state.batchSize; i++) {
            state.onDiskMapOutput.emit("a", 1);
        }
        System.out.println("end to emit");
    }

    @Benchmark
    @Group("emitAndFetch")
    public void readValues(EmptyOnDiskMapOutput state, Control ctrl, Blackhole bh) throws InterruptedException {
        // wait until all messages are processed
        while (!state.onDiskMapOutput.isEmpty()) {
            Thread.onSpinWait();
        }
        state.onDiskMapOutput.close();
        var values = state.onDiskMapOutput.values("a");
        bh.consume(values);
    }

    @Benchmark
    public void keys(FullOnDiskMapOutput state, Blackhole bh) {
        bh.consume(state.onDiskMapOutput.keys());
    }

    @Benchmark
    public void values(FullOnDiskMapOutput state, Blackhole bh) {
        bh.consume(state.onDiskMapOutput.values("a"));
    }
}
