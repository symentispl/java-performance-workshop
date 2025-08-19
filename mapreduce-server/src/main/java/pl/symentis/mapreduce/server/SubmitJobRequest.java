package pl.symentis.mapreduce.server;

import java.util.Map;

public record SubmitJobRequest(String codeUri, Map<String, String> context) {}
