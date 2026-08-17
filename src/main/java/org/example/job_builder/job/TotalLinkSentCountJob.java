package org.example.job_builder.job;

import lombok.experimental.SuperBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.example.event.AskForLink;
import org.example.event.PaymentEvent;
import org.example.job_builder.model.WindowedCount;
import org.example.job_builder.utils.PaymentEventSourceFactory;
import org.example.job_builder.utils.RedisSink;

import java.time.Duration;

@SuperBuilder
public class TotalLinkSentCountJob extends AbstractJob {

    public static final String TOTAL_LINK_SENT_COUNT_JOB = "total-link-sent-count-job";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        var builder = TotalLinkSentCountJob.builder();

        populateCommonFields(builder, params);

        TotalLinkSentCountJob job = builder.build();

        job.run();
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

        events.filter(e -> e instanceof AskForLink)
                .map(e -> 1L)
                .returns(Types.LONG)
                .windowAll(windowAssigner)
                .reduce(Long::sum, new WindowedCountProcessFunction())
                .returns(Types.POJO(WindowedCount.class))
                .sinkTo(new RedisSink(redisHost, redisPort, redisKeyPrefix));

        env.execute(TOTAL_LINK_SENT_COUNT_JOB);
    }

    private static class WindowedCountProcessFunction extends ProcessAllWindowFunction<Long, WindowedCount, TimeWindow> {

        /**
         * @param context  info about processing window
         * @param elements it has only on element(count of link sent) why?
         * @param out      output of process, has only on element in it
         */
        @Override
        public void process(Context context, Iterable<Long> elements, Collector<WindowedCount> out) {
            long count = elements.iterator().next();
            TimeWindow window = context.window();
            out.collect(new WindowedCount(count, window.getStart(), window.getEnd()));
        }

    }

}