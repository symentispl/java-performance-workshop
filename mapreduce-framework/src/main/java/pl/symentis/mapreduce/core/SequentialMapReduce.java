package pl.symentis.mapreduce.core;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static java.lang.String.format;

public class SequentialMapReduce implements MapReduce {

    public static class Builder {

        @SuppressWarnings("rawtypes")
        private Class<? extends MapperOutput> mapperOutputClass = HashMapOutput.class;

        public Builder withMapperOutput(Class<? extends MapperOutput<?, ?>> mapperOutputClass) {
            this.mapperOutputClass = Objects.requireNonNull(mapperOutputClass);
            return this;
        }

        public MapReduce build() {

            @SuppressWarnings("rawtypes")
            Supplier<? extends MapperOutput> supplier = () -> {
                try {
                    return mapperOutputClass.getConstructor().newInstance();
                } catch (InstantiationException
                        | IllegalAccessException
                        | IllegalArgumentException
                        | InvocationTargetException
                        | NoSuchMethodException
                        | SecurityException e) {
                    throw new IllegalArgumentException(format("cannot instatiate mapper output class %s", mapperOutputClass), e);
                }
            };

            return new SequentialMapReduce(supplier);
        }

    }

    @SuppressWarnings("rawtypes")
    private Supplier<? extends MapperOutput> mapperOutputSupplier;

    @SuppressWarnings("rawtypes")
    private SequentialMapReduce(Supplier<? extends MapperOutput> mapperOutputSupplier) {
        this.mapperOutputSupplier = mapperOutputSupplier;
    }

    @Override
    public <In, MK, MV, RK, RV> void run(
            Input<In> input,
            Mapper<In, MK, MV> mapper,
            Reducer<MK, MV, RK, RV> reducer,
            Output<RK, RV> output) {

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
        ;
    }

}
