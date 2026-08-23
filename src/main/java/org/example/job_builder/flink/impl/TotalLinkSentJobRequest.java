package org.example.job_builder.flink.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.AbstractFlinkJobRequest;
import org.example.job_builder.job.impl.TotalLinkSentCountJob;
import org.example.job_builder.model.WindowSpec;

import java.util.List;

@SuperBuilder
public class TotalLinkSentJobRequest extends AbstractFlinkJobRequest<TotalLinkSentCountJob> {

    private final WindowSpec windowSpec;

    @Override
    protected void appendJobSpecificArgs(List<String> args) {
        addWindowSpecToArgs(args, windowSpec);
    }

    @Override
    public String redisKeyPrefix() {
        return "TotalLinkSent-" + windowSpec.windowType() + "-";
    }

    @Override
    public Class<TotalLinkSentCountJob> getJobClass() {
        return TotalLinkSentCountJob.class;
    }

}
