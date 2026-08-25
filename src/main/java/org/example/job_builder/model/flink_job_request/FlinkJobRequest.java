package org.example.job_builder.model.flink_job_request;

import org.example.job_builder.flink.job.AbstractJob;

import java.util.List;

public interface FlinkJobRequest<J extends AbstractJob> {

    String entryClass();

    String redisKeyPrefix();

    List<String> getProgramArgs();

    Class<J> getJobClass();

}
