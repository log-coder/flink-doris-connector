
package org.apache.doris.flink.tools.cdc;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class HeaderKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<String> {

    private final String topic;

    @Nullable
    private final SerializationSchema<String> serializationSchema;

    private static final byte[] EMPTY_HEADER = new byte[0];

    public HeaderKafkaRecordSerializationSchema(String topic, SerializationSchema<String> serializationSchema) {
        this.topic = topic;
        this.serializationSchema = serializationSchema;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
        String headerKey = "";
        String jsonData = element;

        int separatorIndex = element.indexOf('|');
        if (separatorIndex > 0) {
            headerKey = element.substring(0, separatorIndex);
            jsonData = element.substring(separatorIndex + 1);
        }

        byte[] value = serializationSchema.serialize(jsonData);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, value);

        if (headerKey != null && !headerKey.isEmpty()) {
            record.headers().add("table-key", headerKey.getBytes());
        }

        return record;
    }
}
