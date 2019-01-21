package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.model.JobResult;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class DataMigrationService {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobRegistry jobRegistry;

    JobExecution bpMigrationjobExecution = null;
    JobExecution addressMigrationJobExecution =  null;
    JobExecution bprelationshipMigrationJobExecution = null;
    JobExecution customreferenceMigrationJobExecution = null;
    JobExecution externalinfoMigrationJobExecution = null;
    JobExecution marketMigrationJobExecution = null;
    JobExecution objectreplicationstatusMigrationJobExecution = null;


    public void triggerBpMigration() {
        //run all job
        try {
            Job bpMigrationjob = jobRegistry.getJob("bpMigrationJob");
            Job addressMigrationJob = jobRegistry.getJob("addressMigrationJob");
            Job bprelationshipMigrationJob = jobRegistry.getJob("bprelationshipMigrationJob");
            Job customreferenceMigrationJob = jobRegistry.getJob("customreferenceMigrationJob");
            Job externalinfoMigrationJob = jobRegistry.getJob("externalinfoMigrationJob");
            Job marketMigrationJob = jobRegistry.getJob("marketMigrationJob");
            Job objectreplicationstatusMigrationJob = jobRegistry.getJob("objectreplicationstatusMigrationJob");

            bpMigrationjobExecution =  jobLauncher.run(bpMigrationjob, generateJobParams());
            addressMigrationJobExecution =  jobLauncher.run(addressMigrationJob, generateJobParams());
            bprelationshipMigrationJobExecution = jobLauncher.run(bprelationshipMigrationJob, generateJobParams());
            customreferenceMigrationJobExecution = jobLauncher.run(customreferenceMigrationJob, generateJobParams());
            externalinfoMigrationJobExecution = jobLauncher.run(externalinfoMigrationJob, generateJobParams());
            marketMigrationJobExecution = jobLauncher.run(marketMigrationJob, generateJobParams());
            objectreplicationstatusMigrationJobExecution = jobLauncher.run(objectreplicationstatusMigrationJob, generateJobParams());

        } catch (NoSuchJobException e) {
            e.printStackTrace();
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }

    }

    public void triggerOneMigrationJob(String jobName) {
        Job job = null;

        try {
            job = jobRegistry.getJob(jobName);
            jobLauncher.run(job, generateJobParams());
        } catch (NoSuchJobException e) {
            e.printStackTrace();
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }
    }

    public void migrationFailedRecordRetry(String tableName, String pkid) {

    }

    private static JobParameters generateJobParams() {
        return new JobParametersBuilder().addDate("date", new Date()).toJobParameters();
    }

    public List<BatchStatus> getAllJobsStatus() {
        List<BatchStatus> statusList = new ArrayList<>();
        if(null!=bpMigrationjobExecution)
        statusList.add(bpMigrationjobExecution.getStatus());

        if(null!=addressMigrationJobExecution)
        statusList.add(addressMigrationJobExecution.getStatus());

        if(null!=bprelationshipMigrationJobExecution)
        statusList.add(bprelationshipMigrationJobExecution.getStatus());

        if(null!=customreferenceMigrationJobExecution)
        statusList.add(customreferenceMigrationJobExecution.getStatus());

        if(null!=externalinfoMigrationJobExecution)
        statusList.add(externalinfoMigrationJobExecution.getStatus());

        if(null!=marketMigrationJobExecution)
        statusList.add(marketMigrationJobExecution.getStatus());

        if(null!=objectreplicationstatusMigrationJobExecution)
        statusList.add(objectreplicationstatusMigrationJobExecution.getStatus());

        return statusList;
    }

    public JobResult getJobsStatus(String jobName) {

      return null;
    }
}
