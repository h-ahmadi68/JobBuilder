package org.example.job_builder.flink.impl;

import lombok.experimental.SuperBuilder;
import org.example.job_builder.flink.AbstractFlinkJobRequest;
import org.example.job_builder.job.impl.SqlRunnerJob;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

@SuperBuilder
public class SqlJobRequest extends AbstractFlinkJobRequest<SqlRunnerJob> {

    private final String redisKeyPrefix;
    private final String sql;

    @Override
    protected void appendJobSpecificArgs(List<String> args) {
        args.add("--sql");
        args.add(Base64.getEncoder().encodeToString(sql.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public String redisKeyPrefix() {
        return redisKeyPrefix;
    }

    @Override
    public Class<SqlRunnerJob> getJobClass() {
        return SqlRunnerJob.class;
    }

}
