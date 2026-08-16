package org.example.job_builder.utils;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.job_builder.model.WindowedCount;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class RedisSink implements Sink<WindowedCount> {

    private final String host;
    private final int port;
    private final String keyPrefix;

    public RedisSink(String host, int port, String keyPrefix) {
        this.host = host;
        this.port = port;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public SinkWriter<WindowedCount> createWriter(WriterInitContext context) {
        return new RedisSinkWriter(host, port, keyPrefix);
    }

    private static class RedisSinkWriter implements SinkWriter<WindowedCount> {

        private static final DateTimeFormatter FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                        .withZone(ZoneId.systemDefault());

        private final String keyPrefix;
        private final Jedis jedis;

        RedisSinkWriter(String host, int port, String keyPrefix) {
            this.keyPrefix = keyPrefix;
            this.jedis = new Jedis(host, port);
        }

        @Override
        public void write(WindowedCount value, Context context) {
            String start = FORMATTER.format(Instant.ofEpochMilli(value.windowStart()));
            String end = FORMATTER.format(Instant.ofEpochMilli(value.windowEnd()));
            String key = keyPrefix + start + "-to-" + end;

            jedis.set(key, String.valueOf(value.count()));
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() {
            jedis.close();
        }
    }
}