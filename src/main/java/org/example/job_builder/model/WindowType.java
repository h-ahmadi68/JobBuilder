package org.example.job_builder.model;

public enum WindowType {
    TUMBLING, // فقط windowSize لازم است
    SLIDING,  // windowSize + windowSlide لازم است
    SESSION
}
