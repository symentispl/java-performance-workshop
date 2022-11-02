package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Cli;

@Cli(
        name = "mapreduce",
        commands = {Bootstrap.class, Bench.class})
public class Main {
    public static void main(String[] args) {
        new com.github.rvesse.airline.Cli<Runnable>(Main.class).parse(args).run();
    }
}
