package pl.symentis.mapreduce.server;

public class MapReduceServerException extends Exception {
    public MapReduceServerException(String body) {
        super(body);
    }
}
