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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return new NonThreadLocalStopwords(
                    reader.lines().map(collator::getCollationKey).collect(TreeSet::new, TreeSet::add, TreeSet::addAll));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
