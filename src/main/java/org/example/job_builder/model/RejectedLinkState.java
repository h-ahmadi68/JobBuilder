package org.example.job_builder.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public record RejectedLinkState(long sentCount, long rejectedCount, double rejectRatePercent,
                                long windowStart, long windowEnd) implements Serializable {
}
