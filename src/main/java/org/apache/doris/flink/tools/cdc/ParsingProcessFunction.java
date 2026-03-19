package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.tools.cdc.converter.TableNameConverter;
import java.util.HashMap;
import java.util.Map;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {
    protected ObjectMapper objectMapper = new ObjectMapper();
    private transient Map<String, OutputTag<String>> recordOutputTags;
    private TableNameConverter converter;
    private String database;

    // Kafka输出标签
    private static final OutputTag<String> KAFKA_OUTPUT_TAG = new OutputTag<String>("kafka-output") {};

    public ParsingProcessFunction(String database, TableNameConverter converter) {
        this.database = database;
        this.converter = converter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        recordOutputTags = new HashMap<>();
    }

    public static OutputTag<String> createKafkaOutputTag() {
        return KAFKA_OUTPUT_TAG;
    }

    @Override
    public void processElement(
            String record, ProcessFunction<String, Void>.Context context, Collector<Void> collector)
            throws Exception {
        String tableName = getRecordTableName(record);
        String dorisTableName = converter.convert(tableName);
        String dorisDbName = database;
        if (StringUtils.isNullOrWhitespaceOnly(database)) {
            dorisDbName = getRecordDatabaseName(record);
        }
        // 输出到Doris侧流
        context.output(getRecordOutputTag(dorisDbName, dorisTableName), record);

        // 输出到Kafka侧流（每个表一个topic）
        context.output(KAFKA_OUTPUT_TAG, record);
    }

    private String getRecordDatabaseName(String record) throws JsonProcessingException {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        return extractJsonNode(recordRoot.get("source"), "db");
    }

    protected String getRecordTableName(String record) throws Exception {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        return extractJsonNode(recordRoot.get("source"), "table");
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
