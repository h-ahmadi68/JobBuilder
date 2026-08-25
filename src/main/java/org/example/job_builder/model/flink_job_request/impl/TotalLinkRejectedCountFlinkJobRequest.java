package org.example.job_builder.model.flink_job_request.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.job.impl.TotalLinkRejectedCountJob;
import org.example.job_builder.model.WindowSpec;

import java.util.List;

@SuperBuilder
public class TotalLinkRejectedCountFlinkJobRequest extends AbstractFlinkJobRequest<TotalLinkRejectedCountJob> {

    private final WindowSpec windowSpec;

    @Override
    protected void appendJobSpecificArgs(List<String> args) {
        addWindowSpecToArgs(args, windowSpec);
    }

    @Override
    public String redisKeyPrefix() {
        return "TotalRejectedLink-" + windowSpec.windowType() + "-";
    }

    @Override
    public Class<TotalLinkRejectedCountJob> getJobClass() {
        return TotalLinkRejectedCountJob.class;
    }

}
