package org.example.job_builder.model.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import org.example.job_builder.model.WindowSpec;

@Schema(example = """
        {
          "duration": 15,
          "windowSpec": {
            "windowType": "TUMBLING",
            "windowSize": "PT30S",
            "windowSlide": null,
            "sessionGap": null
          }
        }
        """)
@Builder
public record UnopenedLinkCountJobInputDto(long duration, WindowSpec windowSpec) {
}
