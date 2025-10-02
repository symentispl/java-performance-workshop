package pl.symentis.mapreduce.core;

import static java.lang.String.format;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class SequentialMapReduce implements MapReduce {

    public static class Builder {

        @SuppressWarnings("rawtypes")
        private Class<? extends MapperOutput> mapperOutputClass = HashMapOutput.class;

        public Builder withMapperOutput(Class<? extends MapperOutput<?, ?>> mapperOutputClass) {
            this.mapperOutputClass = Objects.requireNonNull(mapperOutputClass);
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder withMapperOutput(String shortClassName) {
            this.mapperOutputClass =
                    (Class<? extends MapperOutput>) findClassByShortName(shortClassName, MapperOutput.class);
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
                    throw new IllegalArgumentException(
                            format("cannot instantiate mapper output class %s", mapperOutputClass), e);
                }
            };

            return new SequentialMapReduce(supplier);
        }

        private static Class<?> findClassByShortName(String shortName, Class<?> interfaceClass) {
            try (ScanResult scanResult = new ClassGraph().enableAllInfo().scan()) {
                ClassInfoList classInfoList = scanResult.getClassesImplementing(interfaceClass);
                return classInfoList.stream()
                        .filter(classInfo -> classInfo.getSimpleName().equals(shortName))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(format(
                                "cannot find class with short name %s implementing %s",
                                shortName, interfaceClass.getName())))
                        .loadClass();
            }
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
        ;
    }
}
