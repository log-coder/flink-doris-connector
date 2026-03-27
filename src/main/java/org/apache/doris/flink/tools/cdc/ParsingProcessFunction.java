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
import org.apache.doris.flink.tools.cdc.utils.CdcRecordProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {
    protected ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<String, OutputTag<String>> recordOutputTags;
    private transient Map<String, OutputTag<String>> kafkaOutputTags;
    private TableNameConverter converter;
    private String database;
    private Pattern kafkaOnlyPattern;

    public ParsingProcessFunction(String database, TableNameConverter converter) {
        this(database, converter, null);
    }

    public ParsingProcessFunction(String database, TableNameConverter converter, String kafkaOnlyTables) {
        this.database = database;
        this.converter = converter;
        this.kafkaOnlyPattern = kafkaOnlyTables == null ? null : Pattern.compile(kafkaOnlyTables);
    }

    /**
     * 判断表是否只发Kafka不写Doris
     */
    private boolean isKafkaOnly(String tableName) {
        if (kafkaOnlyPattern == null) {
            return false;
        }
        return kafkaOnlyPattern.matcher(tableName).matches();
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

        // 判断是否是 kafka-only 表（使用原始表名匹配）
        boolean isKafkaOnly = isKafkaOnly(originalTable);

        // 输出到Doris侧流（使用转换后的表名）- 原始CDC数据（kafka-only表不输出）
        if (!isKafkaOnly) {
            context.output(getRecordOutputTag(dorisDbName, dorisTableName), record);
        }

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
        String idValue = CdcRecordProcessor.getIdValue(root);

        // 处理数据：过滤空字段、datetime补毫秒
        ObjectNode processedData = CdcRecordProcessor.processData(root);

        // 去除ods前缀
        String finalDbName = CdcRecordProcessor.removeOdsPrefix(dbName);
        String finalTableName = CdcRecordProcessor.removeOdsPrefix(tableName);

        // 生成Kafka消息key：库名.表名-id值
        String kafkaKey = finalDbName + "." + finalTableName + "-" + idValue;
        String jsonValue = processedData.toString();

        // 输出格式：key|jsonValue（序列化时会把key设为Kafka消息key）
        return kafkaKey + "|" + jsonValue;
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
