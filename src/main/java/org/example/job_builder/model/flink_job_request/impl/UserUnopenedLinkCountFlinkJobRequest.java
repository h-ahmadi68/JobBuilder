package org.example.job_builder.model.flink_job_request.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.job.impl.UserUnopenedLinkCountJob;
import org.example.job_builder.model.WindowSpec;

import java.util.List;

@SuperBuilder
public class UserUnopenedLinkCountFlinkJobRequest extends AbstractFlinkJobRequest<UserUnopenedLinkCountJob> {

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
    public Class<UserUnopenedLinkCountJob> getJobClass() {
        return UserUnopenedLinkCountJob.class;
    }

}
