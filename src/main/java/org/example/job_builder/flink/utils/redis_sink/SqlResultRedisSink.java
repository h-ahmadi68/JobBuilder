package org.example.job_builder.flink.utils.redis_sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.job_builder.flink.model.SqlWindowResult;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SqlResultRedisSink implements Sink<SqlWindowResult> {

    private final String host;
    private final int port;
    private final String keyPrefix;

    public SqlResultRedisSink(String host, int port, String keyPrefix) {
        this.host = host;
        this.port = port;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public SinkWriter<SqlWindowResult> createWriter(WriterInitContext context) {
        return new SqlResultRedisSinkWriter(host, port, keyPrefix);
    }

    private static class SqlResultRedisSinkWriter implements SinkWriter<SqlWindowResult> {

        private static final DateTimeFormatter FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());

        private final String keyPrefix;
        private final Jedis jedis;

        SqlResultRedisSinkWriter(String host, int port, String keyPrefix) {
            this.keyPrefix = keyPrefix;
            this.jedis = new Jedis(host, port);
        }

        @Override
        public void write(SqlWindowResult value, Context context) {
            String start = FORMATTER.format(Instant.ofEpochMilli(value.windowStart()));
            String end = FORMATTER.format(Instant.ofEpochMilli(value.windowEnd()));
            String key = keyPrefix + start + "-to-" + end;

            jedis.set(key, String.valueOf(value.metricValue()));
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
