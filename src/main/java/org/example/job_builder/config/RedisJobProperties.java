package org.example.job_builder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(prefix = "flink-job.redis")
public record RedisJobProperties(String host, int port) {
}
