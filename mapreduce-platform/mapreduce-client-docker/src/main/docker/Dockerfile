FROM eclipse-temurin:24

ENV PYROSCOPE_APPLICATION_NAME mapreduce-client
ENV PYROSCOPE_SERVER_ADDRESS=http://pyroscope:4040

COPY /maven/libs/agent.jar /app/pyroscope.jar
COPY /maven/libs/mapreduce-server.jar /app/mapreduce-server.jar
COPY /maven/libs/mapreduce-wordcount-bundle.jar /app/mapreduce-wordcount-bundle.jar
COPY /maven/resources/big.txt /resources/big.txt
COPY /maven/config/defaults.cfg /etc/defaults.cfg
RUN mkdir /jobs

ENTRYPOINT ["java","-javaagent:/app/pyroscope.jar","-jar", "/app/mapreduce-server.jar","bench","--jobs-dir","/jobs","--filename","/resources/big.txt","--code-uri", "/app/mapreduce-wordcount-bundle.jar","--config","/etc/defaults.cfg","--job-interval-ms","500"]