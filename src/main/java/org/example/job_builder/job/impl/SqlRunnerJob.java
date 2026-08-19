package org.example.job_builder.job.impl;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.ParameterTool;
import org.example.job_builder.job.AbstractJob;
import org.example.job_builder.model.SqlWindowResult;
import org.example.job_builder.utils.SqlResultRedisSink;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Objects;

@Slf4j
@SuperBuilder
public class SqlRunnerJob extends AbstractJob {

    public static final String SQL_RUNNER_JOB = "sql-runner-job";
    private static final String SOURCE_TABLE_NAME = "payment_events";

    private final String userSql;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        SqlRunnerJob job = SqlRunnerJob.builder()
                .bootstrapServers(params.getRequired("bootstrapServers"))
                .sourceTopic(params.getRequired("sourceTopic"))
                .redisHost(params.getRequired("redisHost"))
                .redisPort(params.getInt("redisPort", 6379))
                .redisKeyPrefix(params.getRequired("redisKeyPrefix"))
                .userSql(decodeSql(params.getRequired("sql")))
                .build();

        log.info("Starting SqlRunnerJob. bootstrapServers={}, sourceTopic={}, redisHost={}, redisPort={}, redisKeyPrefix={}",
                job.bootstrapServers, job.sourceTopic, job.redisHost, job.redisPort, job.redisKeyPrefix);
        log.info("Decoded user SQL:\n{}", job.userSql);

        job.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        registerSourceTable(tEnv, bootstrapServers, sourceTopic);
        log.info("Source table '{}' registered", SOURCE_TABLE_NAME);

        Table resultTable = tEnv.sqlQuery(userSql);
        log.info("Query parsed. Resolved schema: {}", resultTable.getResolvedSchema());

        validateResultSchema(resultTable);
        log.info("Schema validation passed");

        DataStream<Row> resultStream = tEnv.toDataStream(resultTable);
        log.info("Converted Table to DataStream<Row>");

        DataStream<SqlWindowResult> mapped = resultStream.map(row -> {
            log.info("map() received row: {}", row);
            try {
                LocalDateTime windowStart = (LocalDateTime) row.getField("window_start");
                LocalDateTime windowEnd = (LocalDateTime) row.getField("window_end");
                Number metricValue = (Number) row.getField("metric_value");

                log.info("Parsed fields — windowStart={}, windowEnd={}, metricValue={}",
                        windowStart, windowEnd, metricValue);

                long startMillis = Objects.requireNonNull(windowStart).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                long endMillis = Objects.requireNonNull(windowEnd).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

                SqlWindowResult result = new SqlWindowResult(startMillis, endMillis, Objects.requireNonNull(metricValue).doubleValue());
                log.info("Mapped result: {}", result);
                return result;
            } catch (Exception e) {
                log.error("Failed to map row: {}", row, e);
                throw e;
            }
        });

        log.info("Attaching Redis sink with keyPrefix={}", redisKeyPrefix);
        mapped.sinkTo(new SqlResultRedisSink(redisHost, redisPort, redisKeyPrefix));

        log.info("Calling env.execute()");
        env.execute(SQL_RUNNER_JOB);
    }

    private static void registerSourceTable(StreamTableEnvironment tEnv, String bootstrapServers, String sourceTopic) {
        String ddl = String.format("""
                CREATE TABLE %s (
                    `type` STRING,
                    `timestamp` TIMESTAMP(3),
                    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '%s',
                    'properties.bootstrap.servers' = '%s',
                    'properties.group.id' = '%s',
                    'scan.startup.mode' = 'latest-offset',
                    'format' = 'json',
                    'json.ignore-parse-errors' = 'true',
                    'scan.watermark.idle-timeout' = '30s'
                )
                """, SOURCE_TABLE_NAME, sourceTopic, bootstrapServers, SQL_RUNNER_JOB);

        log.info("Registering source table with DDL:\n{}", ddl);
        tEnv.executeSql(ddl);
    }

    private static void validateResultSchema(Table table) {
        var columnNames = table.getResolvedSchema().getColumnNames();
        log.info("Validating schema, columns found: {}", columnNames);

        boolean valid = columnNames.contains("window_start")
                && columnNames.contains("window_end")
                && columnNames.contains("metric_value");

        if (!valid) {
            throw new IllegalArgumentException(
                    "Query result must contain columns: window_start, window_end, metric_value. Found: " + columnNames);
        }
    }

    private static String decodeSql(String encodedSql) {
        return new String(Base64.getDecoder().decode(encodedSql), StandardCharsets.UTF_8);
    }

}