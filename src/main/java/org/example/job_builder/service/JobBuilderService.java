package org.example.job_builder.service;

import lombok.extern.slf4j.Slf4j;
import org.example.job_builder.config.FlinkConfiguration;
import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.model.flink_job_request.FlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.SqlFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.TotalLinkRejectedCountFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.TotalLinkSentCountFlinkJobRequest;
import org.example.job_builder.model.flink_job_request.impl.UserUnopenedLinkCountFlinkJobRequest;
import org.example.job_builder.flink.job.AbstractJob;
import org.example.job_builder.flink.job.impl.SqlRunnerJob;
import org.example.job_builder.flink.job.impl.TotalLinkRejectedCountJob;
import org.example.job_builder.flink.job.impl.TotalLinkSentCountJob;
import org.example.job_builder.flink.job.impl.UserUnopenedLinkCountJob;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.input.SqlUserJobRequest;
import org.example.job_builder.model.input.UserJobRequest;
import org.example.job_builder.model.input.UserUnopenedLinkCountUserJobRequest;
import org.example.job_builder.service.job_request_builder.JobRequestBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


@Slf4j
@Service
public class JobBuilderService {

    private final RestTemplate restTemplate;
    private final FlinkConfiguration flinkConfiguration;
    private final KafkaJobProperties kafkaJobProperties;
    private final RedisJobProperties redisJobProperties;
    private final Map<Class<? extends AbstractJob>, JobRequestBuilder<?, ?, ?>> jobRequestBuilderMap;

    private volatile String cachedJarId;

    public JobBuilderService(RestTemplate restTemplate, FlinkConfiguration flinkConfiguration, KafkaJobProperties kafkaJobProperties, RedisJobProperties redisJobProperties, List<JobRequestBuilder<?, ?, ?>> jobRequestBuilders) {
        this.restTemplate = restTemplate;
        this.flinkConfiguration = flinkConfiguration;
        this.kafkaJobProperties = kafkaJobProperties;
        this.redisJobProperties = redisJobProperties;
        this.jobRequestBuilderMap = jobRequestBuilders.stream()
                .collect(Collectors.toMap(JobRequestBuilder::getJobClass,
                        jobRequestBuilder -> jobRequestBuilder));
    }

    @SuppressWarnings("unchecked")
    public <J extends AbstractJob, R extends UserJobRequest<J>> String submitJob(R request) {
        JobRequestBuilder<J, R, ?> builder = (JobRequestBuilder<J, R, ?>) jobRequestBuilderMap.get(request.getJobClass());

        if (builder == null) {
            throw new IllegalArgumentException("No builder found for job class: " + request.getJobClass());
        }

        FlinkJobRequest<J> flinkRequest = builder.buildFlinkRequest(request);
        return submitJob(flinkRequest);
    }

    @Deprecated()
    public String startUnopenedLinkCountJob(UserUnopenedLinkCountUserJobRequest request) {
        FlinkJobRequest<UserUnopenedLinkCountJob> flinkJobRequest = UserUnopenedLinkCountFlinkJobRequest.builder()
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .duration(request.duration())
                .windowSpec(request.windowSpec())
                .build();

        return submitJob(flinkJobRequest);
    }

    @Deprecated()
    public String submitSql(SqlUserJobRequest submitRequest) {
        FlinkJobRequest<SqlRunnerJob> request = SqlFlinkJobRequest.builder()
                .sql(submitRequest.sql())
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .redisKeyPrefix(submitRequest.redisKeyPrefix())
                .build();

        return submitJob(request);
    }

    @Deprecated()
    public String startTotalLinkSentJob(WindowSpec windowSpec) {
        FlinkJobRequest<TotalLinkSentCountJob> request = TotalLinkSentCountFlinkJobRequest.builder()
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .windowSpec(windowSpec)
                .build();

        return submitJob(request);
    }

    @Deprecated()
    public String startTotalRejectedLinkJob(WindowSpec windowSpec) {
        FlinkJobRequest<TotalLinkRejectedCountJob> request = TotalLinkRejectedCountFlinkJobRequest.builder()
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .windowSpec(windowSpec)
                .build();

        return submitJob(request);
    }

    private String submitJob(FlinkJobRequest<?> request) {
        String jarId = ensureJarUploaded();
        List<String> programArgs = request.getProgramArgs();

        Map<String, Object> response = runJobOnFlink(jarId, request.entryClass(), programArgs);
        String jobId = extractJobId(response);

        log.info("Job submitted to Flink cluster. Entry class: {}, Job ID: {}", request.entryClass(), jobId);
        return jobId;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> runJobOnFlink(String jarId, String entryClass, List<String> programArgs) {
        Map<String, Object> body = Map.of(
                "entryClass", entryClass,
                "programArgsList", programArgs
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        String url = flinkConfiguration.flinkBaseUrl() + "/jars/" + jarId + "/run";
        Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);

        return Objects.requireNonNull(response, "Flink job submission returned an empty response");
    }

    private String extractJobId(Map<String, Object> response) {
        Object jobId = response.get("jobid");
        if (jobId == null) {
            throw new IllegalStateException("Flink response did not contain a 'jobid' field: " + response);
        }
        return jobId.toString();
    }

    private synchronized String ensureJarUploaded() {
        if (cachedJarId != null) {
            return cachedJarId;
        }

        File jarFile = resolveJarFile();
        Map<String, Object> response = uploadJarToFlink(jarFile);
        cachedJarId = extractJarId(response);

        log.info("Job jar uploaded successfully to Flink cluster. ID: {}", cachedJarId);
        return cachedJarId;
    }

    private File resolveJarFile() {
        File jarFile = new File(flinkConfiguration.getJobJarPath());
        if (!jarFile.exists()) {
            throw new IllegalStateException(
                    "Jar file not found: " + jarFile.getAbsolutePath() +
                            " - run 'mvn clean package' from the project root first " +
                            "so this jar gets built (via maven-shade-plugin).");
        }
        return jarFile;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> uploadJarToFlink(File jarFile) {
        HttpEntity<MultiValueMap<String, Object>> request = buildUploadRequest(jarFile);

        Map<String, Object> response = restTemplate.postForObject(
                flinkConfiguration.flinkBaseUrl() + "/jars/upload", request, Map.class);

        return Objects.requireNonNull(response, "Flink jar upload returned an empty response");
    }

    private HttpEntity<MultiValueMap<String, Object>> buildUploadRequest(File jarFile) {
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("jar-file", new FileSystemResource(jarFile));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        return new HttpEntity<>(body, headers);
    }

    private String extractJarId(Map<String, Object> response) {
        String filename = (String) response.get("filename");
        return new File(filename).getName();
    }

}