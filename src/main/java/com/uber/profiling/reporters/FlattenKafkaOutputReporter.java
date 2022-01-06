package com.uber.profiling.reporters;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlattenKafkaOutputReporter extends KafkaOutputReporter {
    @Override
    public void report(String profilerName, Map<String, Object> metrics) {
        Map<String, Object> formattedMetrics = getFormattedMetrics(metrics);
        super.report(profilerName, formattedMetrics);
    }

    @Override
    public String getTopic(String profilerName) {
        String topic = super.getTopic(profilerName);
        return topic.toLowerCase();
    }

    // Format metrics in key=value (line protocol)
    private Map<String, Object> getFormattedMetrics(Map<String, Object> metrics) {
        Map<String, Object> formattedMetrics = new HashMap<>();
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                List listValue = (List) value;
                if (!listValue.isEmpty() && listValue.get(0) instanceof String) {
                    List<String> metricList = (List<String>) listValue;
                    formattedMetrics.put(key, String.join(",", metricList));
                } else if (!listValue.isEmpty() && listValue.get(0) instanceof Map) {
                    List<Map<String, Object>> metricList = (List<Map<String, Object>>) listValue;
                    int num = 1;
                    for (Map<String, Object> metricMap : metricList) {
                        String name = null;
                        if(metricMap.containsKey("name") && metricMap.get("name") != null && metricMap.get("name") instanceof String){
                            name = (String) metricMap.get("name");
                            name = name.replaceAll("\\s", "");
                        }
                        for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                            if(StringUtils.isNotEmpty(name)){
                                formattedMetrics.put(key + "-" + name + "-" + entry1.getKey(), entry1.getValue());
                            }else{
                                formattedMetrics.put(key + "-" + entry1.getKey() + "-" + num, entry1.getValue());
                            }
                        }
                        num++;
                    }
                }
            } else if (value instanceof Map) {
                Map<String, Object> metricMap = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry1 : metricMap.entrySet()) {
                    String key1 = entry1.getKey();
                    Object value1 = entry1.getValue();
                    if (value1 instanceof Map) {
                        Map<String, Object> value2 = (Map<String, Object>) value1;
                        int num = 1;
                        for (Map.Entry<String, Object> entry2 : value2.entrySet()) {
                            formattedMetrics.put(key + "-" + key1 + "-" + entry2.getKey() + "-" + num, entry2.getValue());
                        }
                        num++;
                    }
                }
            } else {
                formattedMetrics.put(key, value);
            }
        }
        return formattedMetrics;
    }
}
