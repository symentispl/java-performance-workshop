package pl.symentis.wordcount.core;

import static java.lang.String.format;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;
import pl.symentis.mapreduce.core.Input;
import pl.symentis.mapreduce.core.Mapper;
import pl.symentis.mapreduce.core.Output;
import pl.symentis.mapreduce.core.Reducer;

public class WordCount {

    public static class Builder {

        private Class<? extends Stopwords> stopwordsClass = NonThreadLocalStopwords.class;
        private Class<? extends StringSplitter> splitterClass = PatternStringSplitter.class;

        public Builder withStopwords(Class<? extends Stopwords> stopwordsClass) {
            this.stopwordsClass = stopwordsClass;
            return this;
        }

        public Builder withStringSplitter(Class<? extends StringSplitter> splitterClass) {
            this.splitterClass = splitterClass;
            return this;
        }

        public WordCount build() {
            try {
                Stopwords stopwords = (Stopwords) stopwordsClass
                        .getMethod("from", InputStream.class)
                        .invoke(stopwordsClass, WordCount.class.getResourceAsStream("stopwords_en.txt"));
                StringSplitter splitter = splitterClass.getDeclaredConstructor().newInstance();
                return new WordCount(stopwords, splitter);
            } catch (IllegalAccessException
                    | IllegalArgumentException
                    | InvocationTargetException
                    | NoSuchMethodException
                    | SecurityException
                    | InstantiationException e) {
                throw new RuntimeException(format("cannot instantiate WordCount dependencies"), e);
            }
        }
    }

    private final Stopwords stopwords;
    private final StringSplitter splitter;

    public WordCount(Stopwords stopwords, StringSplitter splitter) {
        this.stopwords = stopwords;
        this.splitter = splitter;
    }

    public Input<String> input(File file) throws FileNotFoundException {
        return new FileLineInput(file);
    }

    public Input<String> input(InputStream inputStream) {
        return new FileLineInput(inputStream);
    }

    public Mapper<String, String, Long> mapper() {
        return new WordCountMapper(stopwords, splitter);
    }

    public Reducer<String, Long, String, Long> reducer() {
        return new WordCountReducer();
    }

    static final class WordCountReducer implements Reducer<String, Long, String, Long> {

        @Override
        public void reduce(String k, Iterable<Long> input, Output<String, Long> output) {
            Long sum = 0L;
            for (Long l : input) {
                sum += l;
            }
            output.emit(k, sum);
        }
    }

    static final class WordCountMapper implements Mapper<String, String, Long> {

        private final Stopwords stopwords;
        private final StringSplitter splitter;

        WordCountMapper(Stopwords stopwords, StringSplitter splitter) {
            this.stopwords = stopwords;
            this.splitter = splitter;
        }

        @Override
        public void map(String in, Output<String, Long> output) {
            for (String str : splitter.split(in)) {
                if (!stopwords.contains(str)) {
                    output.emit(str, 1L);
                }
            }
        }
    }

    static final class FileLineInput implements Input<String> {

        private final BufferedReader reader;
        private String line;
        private boolean EOF;

        public FileLineInput(File file) throws FileNotFoundException {
            this.reader = new BufferedReader(new FileReader(file));
        }

        public FileLineInput(InputStream input) {
            this.reader = new BufferedReader(new InputStreamReader(input));
        }

        @Override
        public boolean hasNext() {

            if (EOF) {
                return false;
            }

            if (line == null) {
                try {
                    line = reader.readLine();
                    if (line == null) {
                        EOF = true;
                        return false;
                    } else {
                        return true;
                    }

                } catch (IOException e) {
                    throw new IOError(e);
                }
            }

            return true;
        }

        @Override
        public String next() {
            if (hasNext()) {
                String next = line;
                line = null;
                return next;
            }
            throw new NoSuchElementException();
        }
    }
}
