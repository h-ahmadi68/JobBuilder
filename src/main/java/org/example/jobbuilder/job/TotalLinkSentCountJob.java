package org.example.jobbuilder.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.event.PaymentEvent;
import org.example.event.SendLink;
import org.example.jobbuilder.model.WindowType;
import org.example.jobbuilder.service.WindowAssignerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * جاب ساده‌ای که تعداد کل رویدادهای SendLink را در بازه‌های زمانی مشخص‌شده می‌شمرد.
 * نوع window و زمان(های)ش از بیرون (توسط JobBuilderEngine یا main) تزریق می‌شود.
 */
public class TotalLinkSentCountJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SOURCE_TOPIC = "payment_events";

    private final WindowAssigner<Object, TimeWindow> windowAssigner;

    public TotalLinkSentCountJob(WindowAssigner<Object, TimeWindow> windowAssigner) {
        this.windowAssigner = windowAssigner;
    }


    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        WindowSpec.WindowType type = WindowSpec.WindowType.valueOf(params.getRequired("windowType"));
        Duration size = params.has("windowSize") ? Duration.parse(params.get("windowSize")) : null;
        Duration slide = params.has("windowSlide") ? Duration.parse(params.get("windowSlide")) : null;
        Duration gap = params.has("sessionGap") ? Duration.parse(params.get("sessionGap")) : null;

        WindowAssigner<Object, TimeWindow> assigner =
                WindowAssignerFactory.from(new WindowSpec(type, size, slide, gap));

        new TotalLinkSentCountJob(assigner).run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<PaymentEvent> source = PaymentEventSourceFactory.create(
                BOOTSTRAP_SERVERS, SOURCE_TOPIC, "total-link-sent-count-job");

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

    private WindowAssigner<Object, TimeWindow> buildWindowAssigner() {
        return switch (windowType) {
            case TUMBLING -> TumblingEventTimeWindows.of(Time.milliseconds(windowSize.toMillis()));
            case SLIDING -> SlidingEventTimeWindows.of(
                    Time.milliseconds(windowSize.toMillis()),
                    Time.milliseconds(windowSlide.toMillis()));
            case SESSION -> EventTimeSessionWindows.withGap(Time.milliseconds(sessionGap.toMillis()));
        };
    }

}