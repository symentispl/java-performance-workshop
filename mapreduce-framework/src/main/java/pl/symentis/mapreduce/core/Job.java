package pl.symentis.mapreduce.core;

public interface Job
{
    Input<String> input();

    Mapper<String, String, Long> mapper();

    Reducer<String, Long, String, Long> reducer();
}
