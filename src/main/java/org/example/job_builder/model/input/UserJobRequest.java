package org.example.job_builder.model.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import org.example.job_builder.flink.job.AbstractJob;

@Schema(
        discriminatorProperty = "type",
        oneOf = {
                SqlUserJobRequest.class,
                TotalLinkSentCountUserJobRequest.class,
                TotalLinkRejectedCountUserJobRequest.class,
                UserUnopenedLinkCountUserJobRequest.class,
        }
)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlUserJobRequest.class, name = "SQL_RUNNER_JOB"),
        @JsonSubTypes.Type(value = TotalLinkSentCountUserJobRequest.class, name = "TOTAL_LINK_SENT_COUNT_JOB"),
        @JsonSubTypes.Type(value = TotalLinkRejectedCountUserJobRequest.class, name = "TOTAL_LINK_REJECTED_COUNT_JOB"),
        @JsonSubTypes.Type(value = UserUnopenedLinkCountUserJobRequest.class, name = "USER_UNOPENED_LINK_COUNT_JOB"),
})
public interface UserJobRequest<J extends AbstractJob> {

        Class<J> getJobClass();

}
