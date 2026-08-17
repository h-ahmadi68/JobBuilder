package org.example.job_builder.service;

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.job_builder.model.WindowSpec;

public class WindowAssignerFactory {

    public static WindowAssigner<Object, TimeWindow> from(WindowSpec request) {
        validate(request);
        return switch (request.windowType()) {
            case TUMBLING -> TumblingEventTimeWindows.of(request.windowSize());
            case SLIDING -> SlidingEventTimeWindows.of(
                    request.windowSize(),
                    request.windowSlide());
            case SESSION -> EventTimeSessionWindows.withGap(request.sessionGap());
        };
    }

    private static void validate(WindowSpec r) {
        switch (r.windowType()) {
            case TUMBLING -> require(r.windowSize() != null, "windowSize is required for TUMBLING");
            case SLIDING -> {
                require(r.windowSize() != null, "windowSize is required for SLIDING");
                require(r.windowSlide() != null, "windowSlide is required for SLIDING");
            }
            case SESSION -> require(r.sessionGap() != null, "sessionGap is required for SESSION");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

}