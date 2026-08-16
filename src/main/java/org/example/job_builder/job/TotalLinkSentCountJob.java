package org.example.job_builder.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.example.event.AskForLink;
import org.example.event.PaymentEvent;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.WindowType;
import org.example.job_builder.model.WindowedCount;
import org.example.job_builder.service.WindowAssignerFactory;
import org.example.job_builder.utils.PaymentEventSourceFactory;
import org.example.job_builder.utils.RedisSink;

import java.time.Duration;
import java.util.Arrays;

public class TotalLinkSentCountJob {

    public static final String TOTAL_LINK_SENT_COUNT_JOB = "total-link-sent-count-job";

    private final WindowAssigner<Object, TimeWindow> windowAssigner;
    private final String bootstrapServers;
    private final String sourceTopic;
    private final String redisHost;
    private final int redisPort;
    private final String redisKeyPrefix;

    public TotalLinkSentCountJob(WindowAssigner<Object, TimeWindow> windowAssigner,
                                 String bootstrapServers,
                                 String sourceTopic,
                                 String redisHost,
                                 int redisPort,
                                 String redisKeyPrefix) {
        this.windowAssigner = windowAssigner;
        this.bootstrapServers = bootstrapServers;
        this.sourceTopic = sourceTopic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisKeyPrefix = redisKeyPrefix;
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println(Arrays.toString(args));

        WindowSpec windowSpec = parseWindowSpec(params);
        WindowAssigner<Object, TimeWindow> assigner = WindowAssignerFactory.from(windowSpec);

        new TotalLinkSentCountJob(
                assigner,
                params.getRequired("bootstrapServers"),
                params.getRequired("sourceTopic"),
                params.getRequired("redisHost"),
                params.getInt("redisPort", 6379),
                params.getRequired("redisKeyPrefix")
        ).run();
    }

    private static WindowSpec parseWindowSpec(ParameterTool params) {
        return WindowSpec.builder()
                .windowType(WindowType.valueOf(params.getRequired("windowType")))
                .windowSize(params.has("windowSize") ? Duration.parse(params.get("windowSize")) : null)
                .windowSlide(params.has("windowSlide") ? Duration.parse(params.get("windowSlide")) : null)
                .sessionGap(params.has("sessionGap") ? Duration.parse(params.get("sessionGap")) : null)
                .build();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<PaymentEvent> source = PaymentEventSourceFactory.create(
                bootstrapServers, sourceTopic, TOTAL_LINK_SENT_COUNT_JOB);

        DataStream<PaymentEvent> events = env.fromSource(
                source,
                WatermarkStrategy.<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.timestamp().toEpochMilli()),
                "payment_events_source"
        );

        DataStream<WindowedCount> counts = events
                .filter(e -> e instanceof AskForLink)
                .map(e -> 1L)
                .returns(Types.LONG)
                .windowAll(windowAssigner)
                .reduce(Long::sum, new ProcessAllWindowFunction<Long, WindowedCount, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Long> elements, Collector<WindowedCount> out) {
                        long count = elements.iterator().next();
                        TimeWindow window = context.window();
                        out.collect(new WindowedCount(count, window.getStart(), window.getEnd()));
                    }
                })
                .returns(Types.POJO(WindowedCount.class));

        counts.sinkTo(new RedisSink(redisHost, redisPort, redisKeyPrefix));

        env.execute(TOTAL_LINK_SENT_COUNT_JOB);
    }

}