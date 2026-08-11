package org.example.job_builder.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.ParameterTool;
import org.example.event.PaymentEvent;
import org.example.event.SendLink;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.WindowType;
import org.example.job_builder.service.PaymentEventSourceFactory;
import org.example.job_builder.service.WindowAssignerFactory;

import java.time.Duration;


public class TotalLinkSentCountJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "payment_events";
    public static final String TOTAL_LINK_SENT_COUNT_JOB = "total-link-sent-count-job";

    private final WindowAssigner<Object, TimeWindow> windowAssigner;

    public TotalLinkSentCountJob(WindowAssigner<Object, TimeWindow> windowAssigner) {
        this.windowAssigner = windowAssigner;
    }


    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        WindowSpec windowSpec = WindowSpec.builder()
                .windowType(WindowType.valueOf(params.getRequired("windowType")))
                .windowSize(params.has("windowSize") ? Duration.parse(params.get("windowSize")) : null)
                .windowSlide(params.has("windowSlide") ? Duration.parse(params.get("windowSlide")) : null)
                .sessionGap(params.has("sessionGap") ? Duration.parse(params.get("sessionGap")) : null)
                .build();

        WindowAssigner<Object, TimeWindow> assigner =
                WindowAssignerFactory.from(windowSpec);

        new TotalLinkSentCountJob(assigner).run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<PaymentEvent> source = PaymentEventSourceFactory.create(
                BOOTSTRAP_SERVERS, SOURCE_TOPIC, TOTAL_LINK_SENT_COUNT_JOB);

        DataStream<PaymentEvent> events = env.fromSource(
                source,
                WatermarkStrategy.<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.timestamp().toEpochMilli()),
                "payment_events_source"
        );

        DataStream<Long> counts = events
                .filter(e -> e instanceof SendLink)
                .map(e -> 1L)
                .returns(Types.LONG)
                .windowAll(windowAssigner)
                .reduce(Long::sum);

        counts.print();

        env.execute("total-link-sent-count-job");
    }

}