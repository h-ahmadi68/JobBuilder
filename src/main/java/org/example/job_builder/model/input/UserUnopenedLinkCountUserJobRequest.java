package org.example.job_builder.model.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import org.example.job_builder.flink.job.impl.UserUnopenedLinkCountJob;
import org.example.job_builder.model.WindowSpec;

@Schema(example = """
        {
          "type": "USER_UNOPENED_LINK_COUNT_JOB",
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
public record UserUnopenedLinkCountUserJobRequest(long duration,
                                                  WindowSpec windowSpec) implements UserJobRequest<UserUnopenedLinkCountJob> {

    @Override
    public Class<UserUnopenedLinkCountJob> getJobClass() {
        return UserUnopenedLinkCountJob.class;
    }

}
