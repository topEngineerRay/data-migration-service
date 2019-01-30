package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.DataVerificationResult;
import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import com.sap.ngom.datamigration.service.DataVerificationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RequestMapping(value = "/v1")
@Controller
public class DataMigrationController {

    @Autowired DataMigrationService dataMigrationService;

    @Autowired
    DataCleanupService dataCleanupService;

    @Autowired
    DataVerificationService dataVerificationService;

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

    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<DataVerificationResult> migrationTableVerification(@PathVariable("tableName")final String tableName){
        return ResponseEntity.status(200).body(dataVerificationService.tableMigrationResultVerification(tableName));
    }
}
