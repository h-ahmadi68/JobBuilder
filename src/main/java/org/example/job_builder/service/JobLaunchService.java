package org.example.job_builder.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.job_builder.config.FlinkConfiguration;
import org.example.job_builder.model.WindowSpec;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

@RequiredArgsConstructor
@Slf4j
@Service
public class JobLaunchService {

    private static final String TOTAL_ENTRY_CLASS = "org.example.job_builder.job.TotalLinkSentCountJob";

    private final RestTemplate restTemplate;
    private final FlinkConfiguration flinkConfiguration;

    private volatile String cachedJarId;

    public String startTotalLinkSentJob(WindowSpec windowRequest) {
        String jobId = submitJob(windowRequest, TOTAL_ENTRY_CLASS);
        return "TotalRequestCountJob submitted. Job ID: " + jobId;
    }

    private String submitJob(WindowSpec windowSpec, String entryClass) {
        String jarId = ensureJarUploaded();
        String programArgs = buildProgramArgs(windowSpec);

        Map<String, Object> response = runJobOnFlink(jarId, entryClass, programArgs);
        String jobId = extractJobId(response);

        log.info("Job submitted to Flink cluster. Entry class: {}, Job ID: {}", entryClass, jobId);
        return jobId;
    }

    private String buildProgramArgs(WindowSpec request) {
        StringJoiner joiner = new StringJoiner(" ");
        joiner.add("--windowType").add(request.windowType().name());

        appendIfPresent(joiner, "--windowSize", request.windowSize());
        appendIfPresent(joiner, "--windowSlide", request.windowSlide());
        appendIfPresent(joiner, "--sessionGap", request.sessionGap());

        return joiner.toString();
    }

    private void appendIfPresent(StringJoiner joiner, String flag, Duration value) {
        if (value != null) {
            joiner.add(flag).add(String.valueOf(value.toSeconds()));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> runJobOnFlink(String jarId, String entryClass, String programArgs) {
        Map<String, Object> body = Map.of(
                "entryClass", entryClass,
                "programArgs", programArgs
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