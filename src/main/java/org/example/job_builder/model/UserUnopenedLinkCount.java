package org.example.job_builder.model;

import lombok.Builder;

@Builder
public record UserUnopenedLinkCount(String userId, long unopenedLinksCount
        , long windowStart, long windowEnd) {
}
