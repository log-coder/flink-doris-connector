
package org.apache.doris.flink.tools.cdc;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class HeaderKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<String> {

    private final String topic;

    @Nullable
    private final SerializationSchema<String> serializationSchema;

    public HeaderKafkaRecordSerializationSchema(String topic, SerializationSchema<String> serializationSchema) {
        this.topic = topic;
        this.serializationSchema = serializationSchema;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
        String kafkaKey = null;
        String jsonData = element;

        // 格式：key|jsonValue
        int separatorIndex = element.indexOf('|');
        if (separatorIndex > 0) {
            kafkaKey = element.substring(0, separatorIndex);
            jsonData = element.substring(separatorIndex + 1);
        }

        byte[] value = serializationSchema.serialize(jsonData);
        byte[] keyBytes = kafkaKey != null ? kafkaKey.getBytes() : null;

        // kafkaKey 作为消息的 key（用于分区），不设置 header
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, keyBytes, value);

        return record;
    }
}
