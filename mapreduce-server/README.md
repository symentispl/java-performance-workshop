# how to run

mvn package

Start benchmarking process:

    java -jar target/mapreduce-server-0.0.1-SNAPSHOT.jar bench --jobs-dir jobs

Start mapreduce server:

    java -jar target/mapreduce-server-0.0.1-SNAPSHOT.jar bootstrap --jobs-dir jobs
