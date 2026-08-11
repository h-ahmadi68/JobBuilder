package org.example.job_builder.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink extends RichSinkFunction<Long> {

    private final String host;
    private final int port;
    private final String key;

    private transient Jedis jedis;

    public RedisSink(String host, int port, String key) {
        this.host = host;
        this.port = port;
        this.key = key;
    }

    @Override
    public void open(Configuration parameters) {
        jedis = new Jedis(host, port);
    }

    @Override
    public void invoke(Long value, Context context) {
        jedis.set(key, String.valueOf(value));
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

}