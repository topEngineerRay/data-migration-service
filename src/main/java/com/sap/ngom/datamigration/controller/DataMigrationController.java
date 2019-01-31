package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.ResponseMessage;
import com.sap.ngom.datamigration.model.Status;
import com.sap.ngom.datamigration.service.*;

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

    @Autowired
    ManagedInstanceService managedInstanceService;

    @Autowired
    DataVerificationService dataVerificationService;

    @Autowired
    InitializerService initializerService;

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

    @PostMapping("/managed-instances/cleanup")
    public ResponseEntity<Void> managedInstancesClear() {
        try {
            managedInstanceService.deleteAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().build();
    }

    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<ResponseMessage> migrationTableVerification(@PathVariable("tableName")final String tableName){
        return ResponseEntity.status(200).body(dataVerificationService.tableMigrationResultVerification(tableName));
    }

    @PostMapping("/initialization")
    public ResponseEntity<Void> tableInitializeAll() {
        try {
            initializerService.initialize4AllTables();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().build();
    }

    @PostMapping("/initialization/{tableName}")
    public ResponseEntity<Void> tableInitializeOne(@PathVariable("tableName")final String tableName) {
        try {
            initializerService.initialize4OneTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().build();
    }


}
