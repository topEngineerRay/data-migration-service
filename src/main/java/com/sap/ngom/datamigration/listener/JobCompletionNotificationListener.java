package com.sap.ngom.datamigration.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        super.beforeJob(jobExecution);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("Job Name: " + jobExecution.getJobInstance().getJobName() + ", Job Id: " + jobExecution.getJobId() + ", Job Status: " + jobExecution.getStatus().toString());
        if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info("Job fail message: " + jobExecution.getAllFailureExceptions().toString());
        }
    }
}
