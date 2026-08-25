package org.example.job_builder.service.job_request_builder.impl;

import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.flink.job.impl.SqlRunnerJob;
import org.example.job_builder.model.flink_job_request.impl.AbstractFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.SqlFlinkJobRequest;
import org.example.job_builder.model.input.SqlUserJobRequest;
import org.springframework.stereotype.Service;

@Service
public class SqlRunnerJobRequestBuilder extends AbstractJobRequestBuilder<SqlRunnerJob, SqlUserJobRequest, SqlFlinkJobRequest> {

    public SqlRunnerJobRequestBuilder(KafkaJobProperties kafkaJobProperties, RedisJobProperties redisJobProperties) {
        super(kafkaJobProperties, redisJobProperties);
    }

    @Override
    protected AbstractFlinkJobRequest.AbstractFlinkJobRequestBuilder<SqlRunnerJob, ? extends SqlFlinkJobRequest, ?> buildSpecificFields(SqlUserJobRequest request) {
        return SqlFlinkJobRequest.builder()
                .redisKeyPrefix(request.redisKeyPrefix())
                .sql(request.sql());
    }

    @Override
    public Class<SqlRunnerJob> getJobClass() {
        return SqlRunnerJob.class;
    }

}
