package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import com.sap.ngom.datamigration.util.ResponseMessage;
import org.springframework.batch.core.BatchStatus;
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
    public ResponseEntity triggerMigration()
    {
        dataMigrationService.triggerAllMigrationJobs();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        dataMigrationService.triggerOneMigrationJob(tableName);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/data/cleanup/{tableName}")
    public ResponseEntity<ResponseMessage> dataCleanup4OneTable(@PathVariable final String tableName) {
        dataCleanupService.cleanData4OneTable(tableName);

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus("SUCCESS");
        responseMessage.setMessage("Data cleanup successfully done for the table: " + tableName + ".");
        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/data/cleanup")
    public ResponseEntity<ResponseMessage> dataCleanup4AllTables() {
        dataCleanupService.cleanData4AllTables();
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus("SUCCESS");
        responseMessage.setMessage("Data cleanup successfully done for all the tables.");
        return ResponseEntity.ok().body(responseMessage);
    }
}
