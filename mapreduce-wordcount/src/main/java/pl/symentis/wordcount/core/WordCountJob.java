package pl.symentis.wordcount.core;

import pl.symentis.mapreduce.core.Input;
import pl.symentis.mapreduce.core.Job;
import pl.symentis.mapreduce.core.Mapper;
import pl.symentis.mapreduce.core.Reducer;

public class WordCountJob implements Job {

    private Input<String> input;

    private WordCount wordCount;

    public WordCountJob(Input<String> input, WordCount wordCount) {
        this.input = input;
        this.wordCount = wordCount;
    }

    @Override
    public Input<String> input() {
        return input;
    }

    @Override
    public Mapper<String, String, Long> mapper() {
        return wordCount.mapper();
    }

    @Override
    public Reducer<String, Long, String, Long> reducer() {
        return wordCount.reducer();
    }
}
