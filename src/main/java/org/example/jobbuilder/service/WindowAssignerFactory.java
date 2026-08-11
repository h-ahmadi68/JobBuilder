package org.example.jobbuilder.service;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import static org.example.jobbuilder.model.WindowType.*;

public class WindowAssignerFactory {

    public static WindowAssigner<Object, TimeWindow> from(WindowRequest request) {
        validate(request);
        return switch (request.windowType()) {
            case TUMBLING -> TumblingEventTimeWindows.of(Time.milliseconds(request.windowSize().toMillis()));
            case SLIDING -> SlidingEventTimeWindows.of(
                    Time.milliseconds(request.windowSize().toMillis()),
                    Time.milliseconds(request.windowSlide().toMillis()));
            case SESSION -> EventTimeSessionWindows.withGap(Time.milliseconds(request.sessionGap().toMillis()));
        };
    }

    private static void validate(WindowRequest r) {
        switch (r.windowType()) {
            case TUMBLING -> require(r.windowSize() != null, "windowSize برای TUMBLING الزامیه");
            case SLIDING -> {
                require(r.windowSize() != null, "windowSize برای SLIDING الزامیه");
                require(r.windowSlide() != null, "windowSlide برای SLIDING الزامیه");
            }
            case SESSION -> require(r.sessionGap() != null, "sessionGap برای SESSION الزامیه");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}