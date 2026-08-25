package org.example.job_builder.service.job_request_builder.impl;

import lombok.RequiredArgsConstructor;
import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.model.flink_job_request.impl.AbstractFlinkJobRequest;
import org.example.job_builder.flink.job.AbstractJob;
import org.example.job_builder.model.input.UserJobRequest;
import org.example.job_builder.service.job_request_builder.JobRequestBuilder;

@RequiredArgsConstructor
public abstract class AbstractJobRequestBuilder<J extends AbstractJob, R extends UserJobRequest<J>, FR extends AbstractFlinkJobRequest<J>> implements JobRequestBuilder<J, R, FR> {

    private final KafkaJobProperties kafkaJobProperties;
    private final RedisJobProperties redisJobProperties;

    @Override
    public FR buildFlinkRequest(R request) {

        return buildSpecificFields(request)
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .build();
    }

    protected abstract AbstractFlinkJobRequest.AbstractFlinkJobRequestBuilder<J, ? extends FR, ?> buildSpecificFields(R request);

}
