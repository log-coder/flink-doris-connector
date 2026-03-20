
package org.apache.doris.flink.tools.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class KafkaDataProcessFunction extends ProcessFunction<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String database;
    private final TableNameConverter converter;
    private transient Map<String, OutputTag<String>> kafkaOutputTags;

    public KafkaDataProcessFunction(String database, TableNameConverter converter) {
        this.database = database;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kafkaOutputTags = new HashMap<>();
    }



    @Override
    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out)
            throws Exception {
        // 解析JSON
        JsonNode root = objectMapper.readValue(value, JsonNode.class);

        // 获取表名
        String tableName = getRecordTableName(root);
        String dorisTableName = converter.convert(tableName);

        // 获取库名
        String dbName = database;
        if (StringUtils.isNullOrWhitespaceOnly(database)) {
            dbName = getRecordDatabaseName(root);
        }

        // 去除ods前缀
        String originalDbName = dbName;
        String originalTableName = dorisTableName;
        if (dbName != null && dbName.startsWith("ods_")) {
            dbName = dbName.substring(4);
        }
        if (dorisTableName != null && dorisTableName.startsWith("ods_")) {
            dorisTableName = dorisTableName.substring(4);
        }

        // 获取id字段值
        String idValue = getIdValue(root);

        // 处理数据：过滤空字段、datetime补毫秒
        ObjectNode processedData = processData(root);

        // 输出：格式为 "库名.表名-id值|JSON数据"
        String headerKey = dbName + "." + dorisTableName + "-" + idValue;
        String outputValue = processedData.toString();

        // 根据原始表名输出到对应的OutputTag
        String tableKey = originalDbName + "." + originalTableName;
        OutputTag<String> outputTag = kafkaOutputTags.computeIfAbsent(
                tableKey, k -> new OutputTag<>("kafka-" + k) {});
        ctx.output(outputTag, headerKey + "|" + outputValue);
    }

    private String getRecordTableName(JsonNode root) {
        JsonNode source = root.get("source");
        if (source != null && source.get("table") != null) {
            return source.get("table").asText();
        }
        return null;
    }

    private String getRecordDatabaseName(JsonNode root) {
        JsonNode source = root.get("source");
        if (source != null && source.get("db") != null) {
            return source.get("db").asText();
        }
        return null;
    }

    private String getIdValue(JsonNode root) {
        // 尝试从after中获取id字段
        JsonNode after = root.get("after");
        if (after != null && after.has("id")) {
            return after.get("id").asText();
        }
        // 如果没有after，尝试从before中获取
        JsonNode before = root.get("before");
        if (before != null && before.has("id")) {
            return before.get("id").asText();
        }
        // 返回空字符串
        return "";
    }

    /**
     * 处理数据：过滤空字段、datetime补毫秒
     */
    private ObjectNode processData(JsonNode root) {
        ObjectNode result = objectMapper.createObjectNode();

        // 处理before数据
        if (root.has("before") && root.get("before").isObject()) {
            result.set("before", processObjectNode(root.get("before")));
        }

        // 处理after数据
        if (root.has("after") && root.get("after").isObject()) {
            result.set("after", processObjectNode(root.get("after")));
        }

        // 保留其他字段
        Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            if (!"before".equals(key) && !"after".equals(key)) {
                result.set(key, entry.getValue());
            }
        }

        return result;
    }

    /**
     * 处理ObjectNode：过滤空字段、datetime补毫秒
     */
    private ObjectNode processObjectNode(JsonNode node) {
        ObjectNode result = objectMapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            // 过滤空字段：null值和空字符串
            if (value == null || value.isNull()) {
                continue;
            }
            if (value.isTextual() && value.asText().isEmpty()) {
                continue;
            }

            // datetime类型补足毫秒
            if (value.isTextual()) {
                String textValue = value.asText();
                String processedValue = processDatetime(textValue);
                result.put(key, processedValue);
            } else {
                result.set(key, value);
            }
        }

        return result;
    }

    /**
     * 处理datetime类型，补足毫秒
     * 例如: 2026-03-19 13:59:59,1 -> 2026-03-19 13:59:59,100
     */
    private String processDatetime(String value) {
        // 匹配格式: yyyy-MM-dd HH:mm:ss,SSS 或 yyyy-MM-dd HH:mm:ss.SSS
        if (value != null && value.matches("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}[,.]\\d{1,3}")) {
            // 处理 , 分隔符
            if (value.contains(",")) {
                String[] parts = value.split(",");
                if (parts.length == 2) {
                    String datePart = parts[0];
                    String millisPart = parts[1];
                    // 补足3位毫秒
                    while (millisPart.length() < 3) {
                        millisPart = millisPart + "0";
                    }
                    return datePart + "," + millisPart;
                }
            }
            // 处理 . 分隔符
            if (value.contains(".")) {
                String[] parts = value.split("\\.");
                if (parts.length == 2) {
                    String datePart = parts[0];
                    String millisPart = parts[1];
                    // 补足3位毫秒
                    while (millisPart.length() < 3) {
                        millisPart = millisPart + "0";
                    }
                    return datePart + "." + millisPart;
                }
            }
        }
        return value;
    }
}
