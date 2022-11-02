package pl.symentis.wordcount.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.Map;
import pl.symentis.mapreduce.core.Job;
import pl.symentis.mapreduce.core.JobFactory;
import pl.symentis.wordcount.stopwords.ICUThreadLocalStopwords;

public class WordCountJobFactory implements JobFactory {

    @Override
    public Job create(Map<String, String> context) {
        var filename = context.get("filename");
        var wordCount = new WordCount.Builder()
                .withStopwords(ICUThreadLocalStopwords.class)
                .build();
        try {
            return new WordCountJob(wordCount.input(new File(filename)), wordCount);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }
}
