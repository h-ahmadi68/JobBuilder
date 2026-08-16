package org.example.job_builder.model;


import lombok.Builder;

import java.io.Serializable;

@Builder
public record WindowedCount(long count, long windowStart, long windowEnd) implements Serializable {
}
