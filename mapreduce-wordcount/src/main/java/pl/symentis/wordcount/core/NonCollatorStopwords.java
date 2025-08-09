package pl.symentis.wordcount.core;

import java.io.BufferedReader;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeSet;

public class NonCollatorStopwords implements Stopwords {

    private final TreeSet<String> stopwords;

    public static Stopwords from(InputStream inputStream) {
        TreeSet<String> stopwords = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines().collect(() -> stopwords, TreeSet::add, TreeSet::addAll);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return new NonCollatorStopwords(stopwords);
    }

    private NonCollatorStopwords(TreeSet<String> stopwords) {
        this.stopwords = stopwords;
    }

    @Override
    public boolean contains(String str) {
        return stopwords.contains(str);
    }
}
