package org.example.job_builder.utils;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.event.PaymentEvent;
import org.example.job_builder.service.PaymentEventDeserializationSchema;

public class PaymentEventSourceFactory {

    public static KafkaSource<PaymentEvent> create(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<PaymentEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PaymentEventDeserializationSchema())
                .build();
    }

}
