package org.example.job_builder.job.impl;

import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.example.event.OpenLink;
import org.example.event.PaymentEvent;
import org.example.event.SendLink;
import org.example.job_builder.job.AbstractJob;
import org.example.job_builder.model.UserUnopenedLinkCount;
import org.example.job_builder.utils.PaymentEventSourceFactory;
import org.example.job_builder.utils.UserUnopenedCountRedisSink;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@SuperBuilder
public class UnopenedLinkJob extends AbstractJob {

    private final long duration;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        UnopenedLinkJobBuilder<?, ?> builder = UnopenedLinkJob.builder()
                .duration(Long.parseLong(params.getRequired("duration")));
        populateCommonFields(builder, params);

        builder.build().run();
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<PaymentEvent> source =
                PaymentEventSourceFactory.create(bootstrapServers, sourceTopic, getClass().getName());

        DataStreamSource<PaymentEvent> events = env.fromSource(source,
                WatermarkStrategy.<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(30))
                        .withTimestampAssigner((event, ts) -> event.timestamp().toEpochMilli()),
                "payment_events_source");

        DataStream<UnopenedLinkEvent> unopened = events
                .filter(event -> event instanceof SendLink || event instanceof OpenLink)
                .keyBy(event -> {
                    if (event instanceof SendLink sendLink) {
                        return sendLink.paymentLinkId();
                    }

                    if (event instanceof OpenLink openLink) {
                        return openLink.paymentLinkId();
                    }

                    throw new IllegalStateException("Unexpected event type: " + event.getClass());
                })
                .process(new UnopenedLinkDetector(Duration.ofSeconds(duration)));

        unopened.keyBy(UnopenedLinkEvent::userId)
                .window(windowAssigner)
                .aggregate(new UnopenedCountAggregator(), new AttachWindowData())
                .sinkTo(new UserUnopenedCountRedisSink(redisHost, redisPort, redisKeyPrefix));

        env.execute(getClass().getSimpleName());
    }


    private static class UnopenedLinkDetector extends KeyedProcessFunction<String, PaymentEvent, UnopenedLinkEvent> {

        private final long timeoutMillis;
        private transient ValueState<PendingLink> pendingLinkState;

        private UnopenedLinkDetector(Duration timeoutMillis) {
            this.timeoutMillis = timeoutMillis.toMillis();
        }

        @Override
        public void open(OpenContext openContext) {
            pendingLinkState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pending-link", PendingLink.class));
        }

        @Override
        public void processElement(PaymentEvent event, Context ctx,
                                   Collector<UnopenedLinkEvent> out) throws IOException {

            if (event instanceof SendLink sendLink) {
                long sendTs = sendLink.timestamp().toEpochMilli();
                pendingLinkState.update(PendingLink.builder()
                        .userId(sendLink.userId())
                        .paymentLinkId(sendLink.paymentLinkId())
                        .sendTimestamp(sendTs)
                        .build());

                ctx.timerService().registerEventTimeTimer(sendTs + timeoutMillis);

            } else if (event instanceof OpenLink) {
                PendingLink pending = pendingLinkState.value();
                if (pending != null) {
                    ctx.timerService().deleteEventTimeTimer(pending.sendTimestamp() + timeoutMillis);
                    pendingLinkState.clear();
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<UnopenedLinkEvent> out) throws IOException {

            PendingLink pendingLink = pendingLinkState.value();
            if (pendingLink != null) {
                out.collect(UnopenedLinkEvent.builder()
                        .userId(pendingLink.userId())
                        .paymentLinkId(pendingLink.paymentLinkId())
                        .endAt(Instant.ofEpochMilli(timestamp))
                        .build());
                pendingLinkState.clear();
            }
        }
    }

    private static class UnopenedCountAggregator
            implements AggregateFunction<UnopenedLinkEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UnopenedLinkEvent unopenedLinkEvent, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class AttachWindowData extends ProcessWindowFunction<Long, UserUnopenedLinkCount, String, TimeWindow> {

        @Override
        public void process(String userId, Context context,
                            Iterable<Long> aggregatedResult, Collector<UserUnopenedLinkCount> collector) {
            long unopenedLinks = aggregatedResult.iterator().next();

            collector.collect(UserUnopenedLinkCount.builder()
                    .userId(userId)
                    .unopenedLinksCount(unopenedLinks)
                    .windowStart(context.window().getStart())
                    .windowEnd(context.window().getEnd())
                    .build());
        }

    }

    @Builder
    private record UnopenedLinkEvent(String userId, String paymentLinkId, Instant endAt)
            implements Serializable {
    }

    @Builder
    public record PendingLink(String userId, String paymentLinkId, long sendTimestamp)
            implements Serializable {
    }

}