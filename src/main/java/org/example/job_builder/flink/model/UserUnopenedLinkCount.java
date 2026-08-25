package org.example.job_builder.flink.model;

import lombok.Builder;

@Builder
public record UserUnopenedLinkCount(String userId, long unopenedLinksCount
        , long windowStart, long windowEnd) {
}
