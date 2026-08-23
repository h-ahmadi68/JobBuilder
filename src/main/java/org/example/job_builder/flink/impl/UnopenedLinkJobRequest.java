package org.example.job_builder.flink.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.AbstractFlinkJobRequest;
import org.example.job_builder.job.impl.UnopenedLinkJob;
import org.example.job_builder.model.WindowSpec;

import java.util.List;

@SuperBuilder
public class UnopenedLinkJobRequest extends AbstractFlinkJobRequest<UnopenedLinkJob> {

    private final long duration;
    private final WindowSpec windowSpec;

    @Override
    protected void appendJobSpecificArgs(List<String> args) {
        addWindowSpecToArgs(args, windowSpec);

        args.add("--duration");
        args.add(String.valueOf(duration));
    }

    @Override
    public String redisKeyPrefix() {
        return "UnopenedLink-";
    }

    @Override
    public Class<UnopenedLinkJob> getJobClass() {
        return UnopenedLinkJob.class;
    }

}
