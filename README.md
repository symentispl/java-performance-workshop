# How to run benchmarks?

    mvn package # to build code
    java -jar mapreduce-perf/target/benchmarks.jar

# How to profile your code?

You need to have async profiler installed.

Once you have it, you need configure your operating system:

    sysctl kernel.kptr_restrict=0
    sysctl kernel.perf_event_paranoid=1

and run it with:

    java -jar mapreduce-perf/target/benchmarks.jar -prof "async:libPath=<path to async profiler>/build/libasyncProfiler.so;output=flamegraph;event=cpu"

# For non Linux users

You can try to run it under docker with:

    docker run --privileged -u $(id -u ${USER}):$(id -g ${USER}) --rm -v $(pwd):/work symentis/jvmperformance-workshop:latest java
 -jar /work/target/benchmarks.jar -prof "async:libPath=/opt/async-profiler/build/libasyncProfiler.so;output=flamegraph;event=cpu"
