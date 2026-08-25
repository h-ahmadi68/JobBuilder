package org.example.job_builder.flink.model;

import lombok.Builder;

@Builder
public record SqlWindowResult(long windowStart, long windowEnd, double metricValue) {
}
