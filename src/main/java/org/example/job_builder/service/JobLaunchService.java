package org.example.job_builder.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.job_builder.config.FlinkConfiguration;
import org.example.job_builder.config.KafkaJobProperties;
import org.example.job_builder.config.RedisJobProperties;
import org.example.job_builder.flink.FlinkJobRequest;
import org.example.job_builder.flink.impl.SqlJobRequest;
import org.example.job_builder.flink.impl.TotalLinkSentJobRequest;
import org.example.job_builder.flink.impl.TotalRejectedLinkJobRequest;
import org.example.job_builder.flink.impl.UnopenedLinkJobRequest;
import org.example.job_builder.job.impl.SqlRunnerJob;
import org.example.job_builder.job.impl.TotalLinkSentCountJob;
import org.example.job_builder.job.impl.TotalRejectedLinkCountJob;
import org.example.job_builder.job.impl.UnopenedLinkJob;
import org.example.job_builder.model.SqlJobSubmitRequest;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.input.UnopenedLinkCountJobInputDto;
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

@RequiredArgsConstructor
@Slf4j
@Service
public class JobLaunchService {

    private final RestTemplate restTemplate;
    private final FlinkConfiguration flinkConfiguration;
    private final KafkaJobProperties kafkaJobProperties;
    private final RedisJobProperties redisJobProperties;

    private volatile String cachedJarId;

    public String submitUnopenedLinkCountJob(UnopenedLinkCountJobInputDto request) {
        FlinkJobRequest<UnopenedLinkJob> flinkJobRequest = UnopenedLinkJobRequest.builder()
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .duration(request.duration())
                .windowSpec(request.windowSpec())
                .build();

        return submitJob(flinkJobRequest);
    }

    public String submitSql(SqlJobSubmitRequest submitRequest) {
        FlinkJobRequest<SqlRunnerJob> request = SqlJobRequest.builder()
                .sql(submitRequest.sql())
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .redisKeyPrefix(submitRequest.redisKeyPrefix())
                .build();

        return submitJob(request);
    }

    public String startTotalLinkSentJob(WindowSpec windowSpec) {
        FlinkJobRequest<TotalLinkSentCountJob> request = TotalLinkSentJobRequest.builder()
                .bootstrapServers(kafkaJobProperties.bootstrapServers())
                .sourceTopic(kafkaJobProperties.sourceTopic())
                .redisHost(redisJobProperties.host())
                .redisPort(redisJobProperties.port())
                .windowSpec(windowSpec)
                .build();

        return submitJob(request);
    }

    public String startTotalRejectedLinkJob(WindowSpec windowSpec) {
        FlinkJobRequest<TotalRejectedLinkCountJob> request = TotalRejectedLinkJobRequest.builder()
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