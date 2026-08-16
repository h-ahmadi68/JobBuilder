package org.example.job_builder.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.job_builder.model.WindowSpec;
import org.example.job_builder.service.JobLaunchService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

}
