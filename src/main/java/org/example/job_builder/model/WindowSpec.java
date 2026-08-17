package org.example.job_builder.model;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;

import java.time.Duration;

@Builder
public record WindowSpec(@NotNull WindowType windowType,
                         Duration windowSize,
                         Duration windowSlide,
                         Duration sessionGap) {
}
