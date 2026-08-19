package org.example.job_builder.model;

import lombok.Builder;

@Builder
public record SqlJobSubmitRequest(String redisKeyPrefix, String sql) {
}
