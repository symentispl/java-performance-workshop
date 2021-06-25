package pl.symentis.mapreduce.core;

public interface Output<K, V> {

    void emit(K k, V v);

}
