package pl.symentis.wordcount.core;

import java.util.regex.Pattern;

public class PatternStringSplitter implements StringSplitter {

    private static final Pattern PATTERN = Pattern.compile("\\s|\\p{Punct}");

    @Override
    public String[] split(String input) {
        return PATTERN.split(input.toLowerCase());
    }
}
