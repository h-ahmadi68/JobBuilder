package org.example.job_builder.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.job_builder.flink.job.AbstractJob;
import org.example.job_builder.model.input.UserJobRequest;
import org.example.job_builder.service.JobBuilderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v2/jobs")
class JobBuilderControllerV2 {

    private final JobBuilderService jobBuilderService;

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("submit-job")
    public String submitJob(@Valid @RequestBody UserJobRequest<? extends AbstractJob> request) {
        return jobBuilderService.submitJob(request);
    }

}
