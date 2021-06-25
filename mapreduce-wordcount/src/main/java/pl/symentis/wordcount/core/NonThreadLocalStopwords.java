package pl.symentis.wordcount.core;

import java.io.*;
import java.text.CollationKey;
import java.text.Collator;
import java.util.Locale;
import java.util.TreeSet;

public class NonThreadLocalStopwords implements Stopwords {

    private final TreeSet<CollationKey> stopwords;

    public static Stopwords from(InputStream inputStream) {
        Collator collator = Collator.getInstance(Locale.ENGLISH);
        TreeSet<CollationKey> stopwords = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines().map(collator::getCollationKey).collect(() -> stopwords, TreeSet::add, TreeSet::addAll);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return new NonThreadLocalStopwords(stopwords);
    }

    private NonThreadLocalStopwords(TreeSet<CollationKey> stopwords) {
        this.stopwords = stopwords;
    }

    @Override
    public boolean contains(String str) {
        Collator collator = Collator.getInstance(Locale.ENGLISH);
        return stopwords.contains(collator.getCollationKey(str));
    }

}















