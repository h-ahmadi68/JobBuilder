package org.example.job_builder.model.flink_job_request.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.job.AbstractJob;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.flink_job_request.FlinkJobRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SuperBuilder
public abstract class AbstractFlinkJobRequest<J extends AbstractJob> implements FlinkJobRequest<J> {

    private final String bootstrapServers;
    private final String sourceTopic;
    private final String redisHost;
    private final int redisPort;

    @Override
    public final List<String> getProgramArgs() {
        List<String> args = new ArrayList<>();
        args.add("--bootstrapServers");
        args.add(bootstrapServers);
        args.add("--sourceTopic");
        args.add(sourceTopic);
        args.add("--redisHost");
        args.add(redisHost);
        args.add("--redisPort");
        args.add(String.valueOf(redisPort));
        args.add("--redisKeyPrefix");
        args.add(redisKeyPrefix());

        appendJobSpecificArgs(args);
        return args;
    }

    @Override
    public String entryClass() {
        return getJobClass().getCanonicalName();
    }

    protected abstract void appendJobSpecificArgs(List<String> args);

    protected void addWindowSpecToArgs(List<String> args, WindowSpec windowSpec) {
        args.add("--windowType");
        args.add(windowSpec.windowType().name());
        appendIfPresent(args, "--windowSize", windowSpec.windowSize());
        appendIfPresent(args, "--windowSlide", windowSpec.windowSlide());
        appendIfPresent(args, "--sessionGap", windowSpec.sessionGap());
    }

    protected void appendIfPresent(List<String> args, String flag, Duration value) {
        if (value != null) {
            args.add(flag);
            args.add(value.toString());
        }
    }

}
