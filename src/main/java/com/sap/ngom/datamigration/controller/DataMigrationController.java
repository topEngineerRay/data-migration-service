package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@RequestMapping(value = "/v1")
@Controller
public class DataMigrationController {

    @Autowired DataMigrationService dataMigrationService;

    @Autowired
    DataCleanupService dataCleanupService;

    @Bean
    public RestTemplate restTemplate(){
         RestTemplate restTemplate = new RestTemplate();
         return restTemplate;
    }

    @GetMapping("/jobs/{tableName}")
    @ResponseBody
    public JobStatus getOneJobStatus(@PathVariable("tableName")final String tableName) {
        return dataMigrationService.getJobsStatus(tableName);
    }

    @GetMapping("/jobs")
    @ResponseBody
    public List<JobStatus> getAllJobStatus() {
        return dataMigrationService.getAllJobsStatus();
    }

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        return dataMigrationService.triggerOneMigrationJob(tableName);
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

    /*@PostMapping("/container/cleanup")
    public ResponseEntity<Void> clearHDIContainers(){

    }*/
}
