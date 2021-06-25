package pl.symentis.wordcount.stopwords;

import pl.symentis.wordcount.core.Stopwords;

import java.io.*;
import java.text.CollationKey;
import java.text.Collator;
import java.util.Locale;
import java.util.TreeSet;

public class ThreadLocalStopwords implements Stopwords {

    private final ThreadLocal<Collator> threadLocalCollator = new ThreadLocal<Collator>() {

        @Override
        protected Collator initialValue() {
            return (Collator) collator.clone();
        }
    };

    private Collator collator;
    private final TreeSet<CollationKey> stopwords;

    public static Stopwords from(InputStream inputStream) {
        Collator collator = Collator.getInstance(Locale.ENGLISH);
        TreeSet<CollationKey> stopwords = new TreeSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            reader.lines().map(collator::getCollationKey).collect(() -> stopwords, TreeSet::add, TreeSet::addAll);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return new ThreadLocalStopwords(collator, stopwords);
    }

    private ThreadLocalStopwords(Collator collator, TreeSet<CollationKey> stopwords) {
        this.collator = collator;
        this.stopwords = stopwords;
    }

    @Override
    public boolean contains(String str) {

        return stopwords.contains(threadLocalCollator.get().getCollationKey(str));
    }

}