package pl.symentis.mapreduce.core;

public interface MapperOutput<K, V> extends Output<K, V>, Values<K, V> {}
