package org.example.job_builder.model;

import lombok.Builder;

@Builder
public record UserUnOpenedCount(String userId, long unOpenedLinkCount) {
}
