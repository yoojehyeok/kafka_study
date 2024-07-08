package org.example.kafkatest2.kafka.pipeline;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MetricJsonUtils {
    public static double getTotalCpuPercent(String value) {
        JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
        return jsonObject.getAsJsonObject("system")
                .getAsJsonObject("cpu")
                .getAsJsonObject("total")
                .getAsJsonObject("norm")
                .get("pct")
                .getAsDouble();
    }

    public static String getMetricName(String value) {
        JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
        return jsonObject.getAsJsonObject("metricset")
                .get("name")
                .getAsString();
    }

    public static String getHostTimestamp(String value) {
        JsonObject objectValue = JsonParser.parseString(value).getAsJsonObject();
        JsonObject result = objectValue.getAsJsonObject("host");
        result.add("@timestamp", objectValue.get("@timestamp"));
        return result.toString();
    }
}