package pl.symentis.wordcount.core;

import java.util.Iterator;
import java.util.StringTokenizer;

public class StringTokenizerSplitter implements StringSplitter {

    private static final String DELIMITERS = " \t\n\r\f!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    @Override
    public Iterable<String> split(String input) {
        var tokenizer = new StringTokenizer(input, DELIMITERS);
        return () -> new Iterator<>() {
            @Override
            public boolean hasNext() {
                return tokenizer.hasMoreElements();
            }

            @Override
            public String next() {
                return tokenizer.nextToken();
            }
        };
    }
}
