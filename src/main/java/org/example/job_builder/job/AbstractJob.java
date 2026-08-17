package org.example.job_builder.job;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public abstract class AbstractJob {
    protected final String bootstrapServers;
    protected final String sourceTopic;
    protected final String redisHost;
    protected final int redisPort;
    protected final String redisKeyPrefix;
    protected final WindowAssigner<Object, TimeWindow> windowAssigner;

    public AbstractJob(String bootstrapServers, String sourceTopic, String redisHost, int redisPort, String redisKeyPrefix, WindowAssigner<Object, TimeWindow> windowAssigner) {
        this.bootstrapServers = bootstrapServers;
        this.sourceTopic = sourceTopic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKeyPrefix = redisKeyPrefix;
        this.windowAssigner = windowAssigner;
    }
}
