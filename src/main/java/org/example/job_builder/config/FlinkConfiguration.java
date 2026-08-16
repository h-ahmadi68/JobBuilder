package org.example.job_builder.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Getter
@Configuration
public class FlinkConfiguration {

    @Value("${flink.job-manager.host}")
    private String jobManagerHost;

    @Value("${flink.job-manager.port}")
    private int jobManagerPort;

    @Value("${flink.job-jar-path}")
    private String jobJarPath;

    public String flinkBaseUrl() {
        return String.format("http://%s:%d", jobManagerHost, jobManagerPort);
    }

}
