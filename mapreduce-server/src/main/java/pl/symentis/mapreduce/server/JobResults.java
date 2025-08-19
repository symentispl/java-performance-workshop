package pl.symentis.mapreduce.server;

import java.util.Map;

public record JobResults(JobStatus status, Map<String, Long> results) {}
