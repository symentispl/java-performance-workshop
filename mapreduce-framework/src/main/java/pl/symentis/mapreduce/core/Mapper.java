package pl.symentis.mapreduce.core;

public interface Mapper<I, K, V> {

    void map(I in, Output<K, V> output);

}
