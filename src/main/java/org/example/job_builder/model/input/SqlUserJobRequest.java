package org.example.job_builder.model.input;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import org.example.job_builder.flink.job.impl.SqlRunnerJob;

@Schema(example = """
        {
          "type": "SQL_RUNNER_JOB",
          "redisKeyPrefix": "sql-runner:",
          "sql": "SELECT userId, COUNT(*) FROM payment_events WHERE type = 'SEND_LINK' GROUP BY userId"
        }
        """)
@Builder
public record SqlUserJobRequest(String redisKeyPrefix, String sql) implements UserJobRequest<SqlRunnerJob> {

    @Override
    public Class<SqlRunnerJob> getJobClass() {
        return SqlRunnerJob.class;
    }

}
