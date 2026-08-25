package org.example.job_builder.service.job_request_builder.impl;

import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.model.flink_job_request.impl.AbstractFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.TotalLinkSentCountFlinkJobRequest;
import org.example.job_builder.flink.job.impl.TotalLinkSentCountJob;
import org.example.job_builder.model.input.TotalLinkSentCountUserJobRequest;
import org.springframework.stereotype.Service;

@Service
public class TotalLinkSentCountJobRequestBuilder extends AbstractJobRequestBuilder<TotalLinkSentCountJob, TotalLinkSentCountUserJobRequest, TotalLinkSentCountFlinkJobRequest> {

    public TotalLinkSentCountJobRequestBuilder(KafkaJobProperties kafkaJobProperties, RedisJobProperties redisJobProperties) {
        super(kafkaJobProperties, redisJobProperties);
    }

    @Override
    public Class<TotalLinkSentCountJob> getJobClass() {
        return TotalLinkSentCountJob.class;
    }

    @Override
    protected AbstractFlinkJobRequest.AbstractFlinkJobRequestBuilder<TotalLinkSentCountJob, ? extends TotalLinkSentCountFlinkJobRequest, ?> buildSpecificFields(TotalLinkSentCountUserJobRequest request) {

        return TotalLinkSentCountFlinkJobRequest.builder()
                .windowSpec(request.windowSpec());
    }

}
