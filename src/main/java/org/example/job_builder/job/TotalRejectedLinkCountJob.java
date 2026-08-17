package org.example.job_builder.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.example.event.PaymentEvent;
import org.example.event.RejectLink;
import org.example.event.SendLink;
import org.example.job_builder.model.RejectedLinkState;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.WindowType;
import org.example.job_builder.service.WindowAssignerFactory;
import org.example.job_builder.utils.PaymentEventSourceFactory;
import org.example.job_builder.utils.RejectedLinkRedisSink;

import java.time.Duration;

public class TotalRejectedLinkCountJob extends AbstractJob {

    public static final String TOTAL_REJECTED_LINK_COUNT_JOB = "total-rejected-link-count-job";

    public TotalRejectedLinkCountJob(String bootstrapServers, String sourceTopic, String redisHost, int redisPort, String redisKeyPrefix, WindowAssigner<Object, TimeWindow> windowAssigner) {
        super(bootstrapServers, sourceTopic, redisHost, redisPort, redisKeyPrefix, windowAssigner);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        WindowSpec windowSpec = parseWindowSpec(params);
        WindowAssigner<Object, TimeWindow> assigner = WindowAssignerFactory.from(windowSpec);

        new TotalRejectedLinkCountJob(
                params.getRequired("bootstrapServers"),
                params.getRequired("sourceTopic"),
                params.getRequired("redisHost"),
                params.getInt("redisPort", 6379),
                params.getRequired("redisKeyPrefix"),
                assigner
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

        KafkaSource<PaymentEvent> paymentEventKafkaSource = PaymentEventSourceFactory.create(bootstrapServers, sourceTopic, TOTAL_REJECTED_LINK_COUNT_JOB);

        DataStreamSource<PaymentEvent> paymentEvents = env.fromSource(paymentEventKafkaSource,
                WatermarkStrategy.<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.timestamp().toEpochMilli()),
                "payment_events_source");

        paymentEvents.filter(paymentEvent ->
                        paymentEvent instanceof RejectLink || paymentEvent instanceof SendLink)
                .windowAll(windowAssigner)
                .aggregate(new LinkCounterAggregateFunction(), new RejectedLinkStateProcessAllWindowFunction())
                .returns(Types.POJO(RejectedLinkState.class))
                .sinkTo(new RejectedLinkRedisSink(redisHost, redisPort, redisKeyPrefix));

        env.execute(TOTAL_REJECTED_LINK_COUNT_JOB);
    }

    private static class LinkCounterAggregateFunction implements AggregateFunction<PaymentEvent, LinkCounter, LinkCounter> {

        @Override
        public LinkCounter createAccumulator() {
            return new LinkCounter();
        }

        @Override
        public LinkCounter add(PaymentEvent paymentEvent, LinkCounter linkCounter) {

            if (paymentEvent instanceof SendLink)
                linkCounter.totalLinks += 1;
            else if (paymentEvent instanceof RejectLink)
                linkCounter.rejectedLinks += 1;

            return linkCounter;
        }

        @Override
        public LinkCounter getResult(LinkCounter linkCounter) {
            return linkCounter;
        }

        @Override
        public LinkCounter merge(LinkCounter linkCounter, LinkCounter acc1) {

            linkCounter.totalLinks += acc1.totalLinks;
            linkCounter.rejectedLinks += acc1.rejectedLinks;
            return linkCounter;
        }

    }


    private static class RejectedLinkStateProcessAllWindowFunction extends ProcessAllWindowFunction<LinkCounter, RejectedLinkState, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<LinkCounter, RejectedLinkState, TimeWindow>.Context context, Iterable<LinkCounter> iterable, Collector<RejectedLinkState> collector) {

            LinkCounter counter = iterable.iterator().next();
            RejectedLinkState output = RejectedLinkState.builder()
                    .sentCount(counter.totalLinks)
                    .rejectedCount(counter.rejectedLinks)
                    .rejectRatePercent(counter.totalLinks == 0
                            ? 0.0
                            : (counter.rejectedLinks * 100.0) / counter.totalLinks)
                    .windowStart(context.window().getStart())
                    .windowEnd(context.window().getEnd())
                    .build();
            collector.collect(output);
        }

    }

    private static class LinkCounter {
        private long totalLinks;
        private long rejectedLinks;
    }

}
