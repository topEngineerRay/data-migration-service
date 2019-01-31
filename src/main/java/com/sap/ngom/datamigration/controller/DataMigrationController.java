package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RequestMapping
@Controller
public class DataMigrationController {

    @Autowired DataMigrationService dataMigrationService;

    @Autowired
    DataCleanupService dataCleanupService;

    @PostMapping("/jobs")
    public ResponseEntity triggerMigration() {
        return dataMigrationService.triggerAllMigrationJobs();
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
}
