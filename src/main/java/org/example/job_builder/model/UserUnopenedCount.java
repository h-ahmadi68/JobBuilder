package org.example.job_builder.model;

import lombok.Builder;

@Builder
public record UserUnopenedCount(String userId, long unopenedLinksCount) {
}
