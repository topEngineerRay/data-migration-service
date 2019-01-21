package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.JobResult;
import com.sap.ngom.datamigration.service.DataMigrationService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@RequestMapping(value = "/v1/bpMigration")
@Controller
public class DataMigrationController {

    @Autowired DataMigrationService dataMigrationService;

    //get job status
    @GetMapping("/allJobStatus")
    public List<BatchStatus> getJobStatus() {
       return dataMigrationService.getAllJobsStatus();
    }

    //get job status
    @GetMapping("/allJobStatus/{migrationJobName}")
    public JobResult getJobStatus(@PathVariable("migrationJobName")final String jobName) {
        return dataMigrationService.getJobsStatus(jobName);
    }

    @GetMapping("/tiggerAllJob")
    public void triggerBpMigration() {
        dataMigrationService.triggerBpMigration();
    }

    @GetMapping("/migrationOneJob/{migrationJobName}")
    public void migrationJobRetry(@PathVariable("migrationJobName")final String jobName) {
        dataMigrationService.triggerOneMigrationJob(jobName);
    }

    @GetMapping("/migrateSingleRecord")
    public void migrationFailedRecordRetry(@RequestParam final String tableName, @RequestParam final String PKID) {
        dataMigrationService.migrationFailedRecordRetry(tableName, PKID);
    }
}
