package pl.symentis.wordcount.core;

import java.util.Arrays;
import java.util.regex.Pattern;

public class PatternStringSplitter implements StringSplitter {

    private static final Pattern PATTERN = Pattern.compile("\\s|\\p{Punct}");

    @Override
    public Iterable<String> split(String input) {
        return PATTERN.splitAsStream(input)::iterator;
    }
}
