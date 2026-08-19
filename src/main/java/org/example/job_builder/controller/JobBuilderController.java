package org.example.job_builder.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.job_builder.model.SqlJobSubmitRequest;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.service.JobLaunchService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1")
class JobBuilderController {

    private final JobLaunchService jobLaunchService;

    @PostMapping("start-total-link-sent")
    public ResponseEntity<String> startTotalLinkSent(@Valid @RequestBody WindowSpec request) {
        String jobId = jobLaunchService.startTotalLinkSentJob(request);
        return ResponseEntity.accepted().body(jobId);
    }

    @PostMapping("start-total-link-rejected")
    public ResponseEntity<String> startTotalLinkRejected(@Valid @RequestBody WindowSpec request) {
        String jobId = jobLaunchService.startTotalRejectedLinkJob(request);
        return ResponseEntity.accepted().body(jobId);
    }

    @PostMapping("submit-sql")
    public ResponseEntity<String> submitSql(@Valid @RequestBody SqlJobSubmitRequest request) {
        String jobId = jobLaunchService.submitSql(request);
        return ResponseEntity.accepted().body(jobId);
    }

}
