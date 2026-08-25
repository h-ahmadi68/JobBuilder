package org.example.job_builder.model.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import org.example.job_builder.flink.job.impl.TotalLinkSentCountJob;
import org.example.job_builder.model.WindowSpec;

@Schema(example = """
        {
          "type": "TOTAL_LINK_SENT_COUNT_JOB",
          "windowSpec": {
            "windowType": "TUMBLING",
            "windowSize": "PT30S",
            "windowSlide": null,
            "sessionGap": null
          }
        }
        """)
@Builder
public record TotalLinkSentCountUserJobRequest(WindowSpec windowSpec) implements UserJobRequest<TotalLinkSentCountJob> {

    @Override
    public Class<TotalLinkSentCountJob> getJobClass() {
        return TotalLinkSentCountJob.class;
    }

}
