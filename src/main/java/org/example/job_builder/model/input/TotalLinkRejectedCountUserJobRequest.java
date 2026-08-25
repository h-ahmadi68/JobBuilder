package org.example.job_builder.model.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import org.example.job_builder.flink.job.impl.TotalLinkRejectedCountJob;
import org.example.job_builder.model.WindowSpec;

@Schema(example = """
        {
          "type": "TOTAL_LINK_REJECTED_COUNT_JOB",
          "windowSpec": {
            "windowType": "SLIDING",
            "windowSize": "PT1M",
            "windowSlide": "PT30S",
            "sessionGap": null
          }
        }
        """)
@Builder
public record TotalLinkRejectedCountUserJobRequest(
        WindowSpec windowSpec) implements UserJobRequest<TotalLinkRejectedCountJob> {

    @Override
    public Class<TotalLinkRejectedCountJob> getJobClass() {
        return TotalLinkRejectedCountJob.class;
    }

}
