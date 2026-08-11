package org.example.jobbuilder.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
@RequestMapping("/api/v1")
class JobBuilderController {

    @PostMapping
    public ResponseEntity<String> launch(@Valid @RequestBody WindowRequest request) {
        jobLaunchService.launchTotalLinkSentCountJob(request);
        return ResponseEntity.accepted().body("جاب در پس‌زمینه شروع شد");
    }

}
