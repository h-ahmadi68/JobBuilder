package org.example.job_builder.job.impl;

import lombok.Builder;
import lombok.experimental.SuperBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.example.event.OpenLink;
import org.example.event.PaymentEvent;
import org.example.event.SendLink;
import org.example.job_builder.job.AbstractJob;
import org.example.job_builder.model.UserUnOpenedCount;
import org.example.job_builder.utils.PaymentEventSourceFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

@SuperBuilder
public class UnOpenedLinkJob extends AbstractJob {

    private final long duration;

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        UnOpenedLinkJobBuilder<?, ?> builder = UnOpenedLinkJob.builder();
        populateCommonFields(builder, params);

        builder.build().run();
    }

    @Override
    public void run() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<PaymentEvent> source = PaymentEventSourceFactory.create(bootstrapServers, sourceTopic, getClass().getName());

        DataStreamSource<PaymentEvent> events = env.fromSource(source,
                WatermarkStrategy.<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(30))
                        .withTimestampAssigner((event, ts) -> event.timestamp().toEpochMilli()),
                "payment_events_source");

        events.filter(event ->
                        event instanceof SendLink || event instanceof OpenLink)
                .keyBy(event -> {
                    if (event instanceof SendLink sendLink) {
                        return sendLink.paymentLinkId();
                    }

                    if (event instanceof OpenLink openLink) {
                        return openLink.paymentLinkId();
                    }

                    throw new IllegalStateException("Unexpected event type: " + event.getClass());
                })
                .process(new UnopenedLinkDetector(Duration.ofMillis(duration)))
                .returns(TypeInformation.of(UnopenedLinkEvent.class))
                .keyBy(UnopenedLinkEvent::userId)
                .process(new UserUnOpenedCounter())
                .returns(TypeInformation.of(UserUnOpenedCount.class));


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
                                   Collector<UnopenedLinkEvent> out) {

            if (event instanceof SendLink sendLink) {
                long sendTs = sendLink.timestamp().toEpochMilli();
                pendingLinkState.update(
                        new PendingLink(sendLink.userId(), sendLink.paymentLinkId(), sendTs));
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
        public void onTimer(long timestamp, KeyedProcessFunction<String, PaymentEvent, UnopenedLinkEvent>.OnTimerContext ctx, Collector<UnopenedLinkEvent> out) {

            PendingLink pendingLink = pendingLinkState.value();
            if (pendingLink != null) {
                out.collect(UnopenedLinkEvent.builder()
                        .userId(pendingLink.userId())
                        .paymentLinkId(pendingLink.paymentLinkId())
                        .endAt(Instant.ofEpochMilli(pendingLink.sendTimestamp()))
                        .build());
                pendingLinkState.clear();
            }
        }

    }

    private static class UserUnOpenedCounter extends KeyedProcessFunction<String, UnopenedLinkEvent, UserUnOpenedCount> {

        private transient ValueState<Long> countState;

        @Override
        public void open(OpenContext openContext) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("unopened-count", Long.class));
        }

        @Override
        public void processElement(UnopenedLinkEvent unopenedLinkEvent, KeyedProcessFunction<String, UnopenedLinkEvent, UserUnOpenedCount>.Context context, Collector<UserUnOpenedCount> collector) {
            long current = (countState.value() == null ? 0L : countState.value()) + 1;
            countState.update(current);
            collector.collect(UserUnOpenedCount.builder()
                    .userId(unopenedLinkEvent.userId)
                    .unOpenedLinkCount(current)
                    .build());
        }

    }

    @Builder
    private record UnopenedLinkEvent(String userId, String paymentLinkId, Instant endAt) implements Serializable {

    }

    @Builder
    public record PendingLink(String userId, String paymentLinkId, long sendTimestamp)
            implements Serializable {
    }

}
