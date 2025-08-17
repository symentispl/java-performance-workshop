FROM eclipse-temurin:24

ENV PYROSCOPE_APPLICATION_NAME mapreduce-server
ENV PYROSCOPE_SERVER_ADDRESS=http://pyroscope:4040

COPY /maven/libs/agent.jar /app/pyroscope.jar
COPY /maven/libs/mapreduce-server.jar /app/mapreduce-server.jar
COPY /maven/libs/mapreduce-wordcount-bundle.jar /app/mapreduce-wordcount-bundle.jar
COPY /maven/config/defaults.cfg /etc/defaults.cfg
RUN mkdir /jobs

ENTRYPOINT ["java","-javaagent:/app/pyroscope.jar","-jar", "/app/mapreduce-server.jar","bootstrap","--jobs-dir","/jobs","--config","/etc/defaults.cfg"]