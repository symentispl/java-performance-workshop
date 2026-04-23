package pl.symentis.mapreduce.core;

import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class SequentialMapReduce implements MapReduce {

    public static class Builder {


        @SuppressWarnings("rawtypes")
        private Supplier<? extends MapperOutput> mapperOutputSupplier = HashMapOutput::new;



        @SuppressWarnings("rawtypes")
        public Builder withMapperOutputSupplier(Supplier<? extends MapperOutput> supplier) {
            this.mapperOutputSupplier = requireNonNull(supplier);
            return this;
        }

        public MapReduce build() {
            return new SequentialMapReduce(mapperOutputSupplier);
        }


    }

    @SuppressWarnings("rawtypes")
    private final Supplier<? extends MapperOutput> mapperOutputSupplier;

    @SuppressWarnings("rawtypes")
    private SequentialMapReduce(Supplier<? extends MapperOutput> mapperOutputSupplier) {
        this.mapperOutputSupplier = mapperOutputSupplier;
    }

    @Override
    public <In, MK, MV, RK, RV> void run(
            Input<In> input, Mapper<In, MK, MV> mapper, Reducer<MK, MV, RK, RV> reducer, Output<RK, RV> output) {

        @SuppressWarnings("unchecked")
        MapperOutput<MK, MV> mapperOutput = mapperOutputSupplier.get();

        while (input.hasNext()) {
            mapper.map(input.next(), mapperOutput);
        }

        Set<MK> keys = mapperOutput.keys();
        for (MK key : keys) {
            reducer.reduce(key, () -> mapperOutput.values(key), output);
        }
    }

    @Override
    public void shutdown() {
    }
}
