package pl.symentis.wordcount.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.TreeSet;

public class NonCollatorStopwords implements Stopwords {

    private final TreeSet<String> stopwords;

    public static Stopwords from(InputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return new NonCollatorStopwords(reader.lines().collect(TreeSet::new, TreeSet::add, TreeSet::addAll));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read stop words", e);
        }
    }

    private NonCollatorStopwords(TreeSet<String> stopwords) {
        this.stopwords = stopwords;
    }

    @Override
    public boolean contains(String str) {
        return stopwords.contains(str);
    }
}
