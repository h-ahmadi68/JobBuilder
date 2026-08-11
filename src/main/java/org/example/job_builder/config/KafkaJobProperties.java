package org.example.job_builder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flink-job.kafka")
public record KafkaJobProperties(String bootstrapServers, String sourceTopic) {

}
