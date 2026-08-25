package org.example.job_builder.service.job_request_builder.impl;

import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.model.flink_job_request.impl.AbstractFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.UserUnopenedLinkCountFlinkJobRequest;
import org.example.job_builder.flink.job.impl.UserUnopenedLinkCountJob;
import org.example.job_builder.model.input.UserUnopenedLinkCountUserJobRequest;
import org.springframework.stereotype.Service;

@Service
public class UserUnopenedLinkCountJobRequestBuilder extends AbstractJobRequestBuilder<UserUnopenedLinkCountJob, UserUnopenedLinkCountUserJobRequest, UserUnopenedLinkCountFlinkJobRequest> {

    public UserUnopenedLinkCountJobRequestBuilder(KafkaJobProperties kafkaJobProperties, RedisJobProperties redisJobProperties) {
        super(kafkaJobProperties, redisJobProperties);
    }

    @Override
    protected AbstractFlinkJobRequest.AbstractFlinkJobRequestBuilder<UserUnopenedLinkCountJob, ? extends UserUnopenedLinkCountFlinkJobRequest, ?> buildSpecificFields(UserUnopenedLinkCountUserJobRequest request) {
        return UserUnopenedLinkCountFlinkJobRequest.builder()
                .duration(request.duration())
                .windowSpec(request.windowSpec());
    }

    @Override
    public Class<UserUnopenedLinkCountJob> getJobClass() {
        return UserUnopenedLinkCountJob.class;
    }

}
