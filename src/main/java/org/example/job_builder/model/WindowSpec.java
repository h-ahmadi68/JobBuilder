package org.example.job_builder.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

import java.time.Duration;

@Schema(example = """
        {
          "windowType": "TUMBLING",
          "windowSize": "PT30M",
          "windowSlide": null,
          "sessionGap": null
        }
        """)
@Builder
public record WindowSpec(@NotNull WindowType windowType,
                         Duration windowSize,
                         Duration windowSlide,
                         Duration sessionGap) {
}
