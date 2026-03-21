package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {
    protected ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<String, OutputTag<String>> recordOutputTags;
    private transient Map<String, OutputTag<String>> kafkaOutputTags;
    private TableNameConverter converter;
    private String database;

    // datetime格式正则
    private static final Pattern DATETIME_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}[,.]\\d{1,3}");

    public ParsingProcessFunction(String database, TableNameConverter converter) {
        this.database = database;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        recordOutputTags = new HashMap<>();
        kafkaOutputTags = new HashMap<>();
    }

    @Override
    public void processElement(
            String record, ProcessFunction<String, Void>.Context context, Collector<Void> collector)
            throws Exception {
        // 获取原始db和table（从source）
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String originalDb = extractJsonNode(recordRoot.get("source"), "db");
        String originalTable = extractJsonNode(recordRoot.get("source"), "table");

        // Doris表名（可能经过converter转换）
        String dorisTableName = converter.convert(originalTable);
        String dorisDbName = StringUtils.isNullOrWhitespaceOnly(database) ? originalDb : database;

        // 输出到Doris侧流（使用转换后的表名）- 原始CDC数据
        context.output(getRecordOutputTag(dorisDbName, dorisTableName), record);

        // 输出到Kafka侧流（使用原始db.table命名）- 处理后的数据
        String kafkaTableKey = originalDb + "." + originalTable;
        String kafkaRecord = processKafkaRecord(recordRoot, originalDb, dorisTableName);
        context.output(getKafkaOutputTag(kafkaTableKey), kafkaRecord);
    }

    /**
     * 处理Kafka数据：过滤空字段、datetime补毫秒、生成消息key
     * 输出格式："key|value"，其中key用于Kafka消息key，value是JSON数据
     */
    private String processKafkaRecord(JsonNode root, String dbName, String tableName) throws Exception {
        // 获取id字段值
        String idValue = getIdValue(root);

        // 处理数据：过滤空字段、datetime补毫秒
        ObjectNode processedData = processData(root);

        // 去除ods前缀
        String finalDbName = dbName;
        String finalTableName = tableName;
        if (dbName != null && dbName.startsWith("ods_")) {
            finalDbName = dbName.substring(4);
        }
        if (tableName != null && tableName.startsWith("ods_")) {
            finalTableName = tableName.substring(4);
        }

        // 生成Kafka消息key：库名.表名-id值
        String kafkaKey = finalDbName + "." + finalTableName + "-" + idValue;
        String jsonValue = processedData.toString();

        // 输出格式：key|jsonValue（序列化时会把key设为Kafka消息key）
        return kafkaKey + "|" + jsonValue;
    }

    /**
     * 获取id字段值
     */
    private String getIdValue(JsonNode root) {
        JsonNode after = root.get("after");
        if (after != null && after.has("id")) {
            return after.get("id").asText();
        }
        JsonNode before = root.get("before");
        if (before != null && before.has("id")) {
            return before.get("id").asText();
        }
        return "";
    }

    /**
     * 处理数据：过滤空字段、datetime补毫秒，精简source和去除ts_ms、transaction
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

        // 处理source：只保留db和table
        if (root.has("source") && root.get("source").isObject()) {
            JsonNode sourceNode = root.get("source");
            ObjectNode simplifiedSource = objectMapper.createObjectNode();
            if (sourceNode.has("db")) {
                simplifiedSource.set("db", sourceNode.get("db"));
            }
            if (sourceNode.has("table")) {
                simplifiedSource.set("table", sourceNode.get("table"));
            }
            result.set("source", simplifiedSource);
        }

        // 保留op字段
        if (root.has("op")) {
            result.set("op", root.get("op"));
        }

        return result;
    }

    /**
     * 处理ObjectNode：过滤空字段、datetime补毫秒
     */
    private ObjectNode processObjectNode(JsonNode node) {
        if (node == null || !node.isObject()) {
            return objectMapper.createObjectNode();
        }

        ObjectNode result = objectMapper.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();

            // 过滤空字段：null值、空字符串、纯空格字符串
            if (value == null || value.isNull()) {
                continue;
            }
            if (value.isMissingNode()) {
                continue;
            }
            if (value.isTextual()) {
                String textValue = value.asText();
                if (textValue.isEmpty() || textValue.trim().isEmpty()) {
                    continue;
                }
                // datetime类型补足毫秒
                String processedValue = processDatetime(textValue);
                result.put(key, processedValue);
                continue;
            }

            result.set(key, value);
        }

        return result;
    }

    /**
     * 处理datetime类型，补足毫秒
     */
    private String processDatetime(String value) {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return value;
        }

        // 处理 , 分隔符
        if (value.contains(",")) {
            String[] parts = value.split(",");
            if (parts.length == 2) {
                String datePart = parts[0];
                String millisPart = parts[1];
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
                while (millisPart.length() < 3) {
                    millisPart = millisPart + "0";
                }
                return datePart + "." + millisPart;
            }
        }
        return value;
    }

    /**
     * 根据原始db.table获取Kafka OutputTag
     */
    private OutputTag<String> getKafkaOutputTag(String tableKey) {
        return kafkaOutputTags.computeIfAbsent(
                tableKey, k -> new OutputTag<>("kafka-" + k) {});
    }

    protected String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private OutputTag<String> getRecordOutputTag(String databaseName, String tableName) {
        String tableIdentifier = databaseName + "." + tableName;
        return recordOutputTags.computeIfAbsent(
                tableIdentifier, k -> createRecordOutputTag(databaseName, tableName));
    }

    public static OutputTag<String> createRecordOutputTag(String databaseName, String tableName) {
        return new OutputTag<String>(String.format("record-%s-%s", databaseName, tableName)) {};
    }
}
