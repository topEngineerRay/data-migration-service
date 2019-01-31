package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.ResponseMessage;
import com.sap.ngom.datamigration.model.Status;
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
    public ResponseEntity<ResponseMessage> dataCleanup4OneTable(@PathVariable final String tableName) {
        dataCleanupService.cleanData4OneTable(tableName);

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Data cleanup successfully done for the table: " + tableName + ".");
        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/data/cleanup")
    public ResponseEntity<ResponseMessage> dataCleanup4AllTables() {
        dataCleanupService.cleanData4AllTables();
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Data cleanup successfully done for all the tables.");
        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<ResponseMessage> migrationTableVerification(@PathVariable("tableName")final String tableName){
        return ResponseEntity.status(200).body(dataVerificationService.tableMigrationResultVerification(tableName));
    }
}
