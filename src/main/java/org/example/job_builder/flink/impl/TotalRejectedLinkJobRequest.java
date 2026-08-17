package org.example.job_builder.flink.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.AbstractFlinkJobRequest;
import org.example.job_builder.job.impl.TotalRejectedLinkCountJob;
import org.example.job_builder.model.WindowSpec;

import java.util.List;

@SuperBuilder
public class TotalRejectedLinkJobRequest extends AbstractFlinkJobRequest<TotalRejectedLinkCountJob> {

    private final WindowSpec windowSpec;

    @Override
    protected void appendJobSpecificArgs(List<String> args) {
        args.add("--windowType");
        args.add(windowSpec.windowType().name());
        appendIfPresent(args, "--windowSize", windowSpec.windowSize());
        appendIfPresent(args, "--windowSlide", windowSpec.windowSlide());
        appendIfPresent(args, "--sessionGap", windowSpec.sessionGap());
    }

    @Override
    public String redisKeyPrefix() {
        return "TotalRejectedLink-" + windowSpec.windowType() + "-";
    }

    @Override
    public Class<TotalRejectedLinkCountJob> getJobClass() {
        return TotalRejectedLinkCountJob.class;
    }

}
