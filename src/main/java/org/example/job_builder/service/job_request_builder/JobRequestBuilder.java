package org.example.job_builder.service.job_request_builder;

import org.example.job_builder.model.flink_job_request.FlinkJobRequest;
import org.example.job_builder.flink.job.AbstractJob;
import org.example.job_builder.model.input.UserJobRequest;

public interface JobRequestBuilder<J extends AbstractJob, R extends UserJobRequest<J>, FR extends FlinkJobRequest<J>> {

    Class<J> getJobClass();

    FR buildFlinkRequest(R request);
}
