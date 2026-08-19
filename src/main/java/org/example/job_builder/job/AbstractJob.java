package org.example.job_builder.job;

import lombok.experimental.SuperBuilder;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.ParameterTool;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.WindowType;
import org.example.job_builder.service.WindowAssignerFactory;

import java.time.Duration;

@SuperBuilder
public abstract class AbstractJob {
    protected final String bootstrapServers;
    protected final String sourceTopic;
    protected final String redisHost;
    protected final int redisPort;
    protected final String redisKeyPrefix;
    protected final WindowAssigner<Object, TimeWindow> windowAssigner;

    public abstract void run() throws Exception;

    protected static void populateCommonFields(AbstractJobBuilder<?, ?> builder, ParameterTool params) {
        WindowSpec windowSpec = parseWindowSpec(params);

        WindowAssigner<Object, TimeWindow> assigner = WindowAssignerFactory.from(windowSpec);

        builder
                .bootstrapServers(params.getRequired("bootstrapServers"))
                .sourceTopic(params.getRequired("sourceTopic"))
                .redisHost(params.getRequired("redisHost"))
                .redisPort(params.getInt("redisPort", 6379))
                .redisKeyPrefix(params.getRequired("redisKeyPrefix"))
                .windowAssigner(assigner);
    }

    private static WindowSpec parseWindowSpec(ParameterTool params) {

        return WindowSpec.builder()
                .windowType(WindowType.valueOf(params.getRequired("windowType")))
                .windowSize(params.has("windowSize") ? Duration.parse(params.get("windowSize")) : null)
                .windowSlide(params.has("windowSlide") ? Duration.parse(params.get("windowSlide")) : null)
                .sessionGap(params.has("sessionGap") ? Duration.parse(params.get("sessionGap")) : null)
                .build();
    }

}
