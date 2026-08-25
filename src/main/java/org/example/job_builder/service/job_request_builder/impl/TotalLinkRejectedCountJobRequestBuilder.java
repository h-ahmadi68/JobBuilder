package org.example.job_builder.service.job_request_builder.impl;

import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.model.flink_job_request.impl.AbstractFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.TotalLinkRejectedCountFlinkJobRequest;
import org.example.job_builder.flink.job.impl.TotalLinkRejectedCountJob;
import org.example.job_builder.model.input.TotalLinkRejectedCountUserJobRequest;
import org.springframework.stereotype.Service;

@Service
public class TotalLinkRejectedCountJobRequestBuilder extends AbstractJobRequestBuilder<TotalLinkRejectedCountJob, TotalLinkRejectedCountUserJobRequest, TotalLinkRejectedCountFlinkJobRequest> {

    public TotalLinkRejectedCountJobRequestBuilder(KafkaJobProperties kafkaJobProperties, RedisJobProperties redisJobProperties) {
        super(kafkaJobProperties, redisJobProperties);
    }

    @Override
    protected AbstractFlinkJobRequest.AbstractFlinkJobRequestBuilder<TotalLinkRejectedCountJob, ? extends TotalLinkRejectedCountFlinkJobRequest, ?> buildSpecificFields(TotalLinkRejectedCountUserJobRequest request) {

        return TotalLinkRejectedCountFlinkJobRequest.builder()
                .windowSpec(request.windowSpec());
    }

    @Override
    public Class<TotalLinkRejectedCountJob> getJobClass() {
        return TotalLinkRejectedCountJob.class;
    }

}
