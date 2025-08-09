package pl.symentis.mapreduce.core;

public interface Reducer<MapperKey, MapperValue, ReducerKey, ReducerValue> {

    void reduce(MapperKey k, Iterable<MapperValue> input, Output<ReducerKey, ReducerValue> output);
}
