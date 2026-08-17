package org.example.job_builder.flink;

import org.example.job_builder.job.AbstractJob;

import java.util.List;

public interface FlinkJobRequest<J extends AbstractJob> {

    String entryClass();

    String redisKeyPrefix();

    List<String> getProgramArgs();

    Class<J> getJobClass();

}
