package org.example.job_builder.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.model.input.SqlUserJobRequest;
import org.example.job_builder.model.input.UserUnopenedLinkCountUserJobRequest;
import org.example.job_builder.service.JobBuilderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @deprecated use v2 API
 */
@Deprecated(forRemoval = true)
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/jobs")
class JobBuilderControllerV1 {

    private final JobBuilderService jobBuilderService;

    @PostMapping("start-total-link-sent")
    public ResponseEntity<String> startTotalLinkSent(@Valid @RequestBody WindowSpec request) {
        String jobId = jobBuilderService.startTotalLinkSentJob(request);
        return ResponseEntity.accepted().body(jobId);
    }

    @PostMapping("start-total-link-rejected")
    public ResponseEntity<String> startTotalLinkRejected(@Valid @RequestBody WindowSpec request) {
        String jobId = jobBuilderService.startTotalRejectedLinkJob(request);
        return ResponseEntity.accepted().body(jobId);
    }

    @PostMapping("submit-sql")
    public ResponseEntity<String> submitSql(@Valid @RequestBody SqlUserJobRequest request) {
        String jobId = jobBuilderService.submitSql(request);
        return ResponseEntity.accepted().body(jobId);
    }

    @PostMapping("satrt-unopened-link-job")
    public ResponseEntity<String> startUnopenedLinkJob(@Valid @RequestBody UserUnopenedLinkCountUserJobRequest request) {
        String jobId = jobBuilderService.startUnopenedLinkCountJob(request);
        return ResponseEntity.accepted().body(jobId);
    }

}
