package org.example.job_builder;

import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties({KafkaJobProperties.class, RedisJobProperties.class})
@SpringBootApplication
public class JobBuilderApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobBuilderApplication.class, args);
    }

}
