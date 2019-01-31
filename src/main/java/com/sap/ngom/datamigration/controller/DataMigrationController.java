package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import com.sap.ngom.datamigration.util.MessageBuilder;
import org.springframework.batch.core.BatchStatus;
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

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        return dataMigrationService.triggerOneMigrationJob(tableName);
    }

    @PostMapping("/data/cleanup/{tableName}")
    public ResponseEntity<MessageBuilder> dataCleanup4OneTable(@PathVariable final String tableName) {
        dataCleanupService.cleanData4OneTable(tableName);

        MessageBuilder messageBuilder = new MessageBuilder();
        messageBuilder.setStatus("SUCCESS");
        messageBuilder.setMessage("Data cleanup successfully done for the table: " + tableName + ".");
        return ResponseEntity.ok().body(messageBuilder);
    }

    @PostMapping("/data/cleanup")
    public ResponseEntity<MessageBuilder> dataCleanup4AllTables() {
        dataCleanupService.cleanData4AllTables();
        MessageBuilder messageBuilder = new MessageBuilder();
        messageBuilder.setStatus("SUCCESS");
        messageBuilder.setMessage("Data cleanup successfully done for all the tables.");
        return ResponseEntity.ok().body(messageBuilder);
    }
}
