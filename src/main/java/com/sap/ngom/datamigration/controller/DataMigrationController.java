package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.JobResult;
import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequestMapping(value = "/v1")
@Controller
public class DataMigrationController {

    @Autowired DataMigrationService dataMigrationService;

    @Autowired
    DataCleanupService dataCleanupService;

    //get job status
    @GetMapping("/{serviceName}/allJobStatus")
    public List<BatchStatus> getJobStatus(@PathVariable("serviceName")final String serviceName) {
       return dataMigrationService.getAllJobsStatus(serviceName);
    }

    //get job status
    @GetMapping("/allJobStatus/{serviceName}/{migrationJobName}")
    public JobResult getJobStatus(@PathVariable("serviceName")final String serviceName,
                                  @PathVariable("migrationJobName")final String jobName) {
        return dataMigrationService.getJobsStatus(jobName);
    }

    @GetMapping("/tiggerAllJob/{serviceName}")
    public void triggerBpMigration(@PathVariable("serviceName")final String serviceName) {
        dataMigrationService.triggerBpMigration(serviceName);
    }

    @GetMapping("/migrationOneJob/{serviceName}/{migrationJobName}")
    public void migrationJobRetry(@PathVariable("serviceName")final String serviceName,
                                  @PathVariable("migrationJobName")final String jobName) {
        dataMigrationService.triggerOneMigrationJob(serviceName,jobName);
    }

    @GetMapping("/migrateSingleRecord")
    public void migrationFailedRecordRetry(@RequestParam final String tableName, @RequestParam final String PKID) {
        dataMigrationService.migrationFailedRecordRetry(tableName, PKID);
    }

    @PostMapping("/data/cleanup/{tableName}")
    public ResponseEntity<Void> dataCleanup4OneTable(@PathVariable final String tableName) {
        dataCleanupService.cleanData4OneTable(tableName);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/data/cleanup")
    public ResponseEntity<Void> dataCleanup4AllTables() {
        dataCleanupService.cleanData4AllTables();
        return ResponseEntity.ok().build();
    }
}
