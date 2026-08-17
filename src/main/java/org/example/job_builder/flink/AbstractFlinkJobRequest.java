package org.example.job_builder.flink;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.job.AbstractJob;

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

    protected void appendIfPresent(List<String> args, String flag, Duration value) {
        if (value != null) {
            args.add(flag);
            args.add(value.toString());
        }
    }

}
