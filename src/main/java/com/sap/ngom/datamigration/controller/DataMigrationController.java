package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.model.MigrateRecord;
import com.sap.ngom.datamigration.model.ResponseMessage;
import com.sap.ngom.datamigration.model.Status;
import com.sap.ngom.datamigration.service.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RequestMapping
@Controller
public class DataMigrationController {

    public static final String TRIGGER_DATA_MIGRATION_SUCCESSFULLY = "Trigger data migration successfully.";

    @Autowired DataMigrationService dataMigrationService;

    @Autowired
    DataCleanupService dataCleanupService;

    @Autowired
    ManagedInstanceService managedInstanceService;

    @Autowired
    DataVerificationService dataVerificationService;

    @Autowired
    InitializerService initializerService;


    @GetMapping("/jobs/{tableName}")
    @ResponseBody
    public JobStatus getOneJobStatus(@PathVariable("tableName")final String tableName) {
        return dataMigrationService.getJobStatus(tableName);
    }

    @GetMapping("/jobs")
    @ResponseBody
    public List<JobStatus> getAllJobStatus() {
        return dataMigrationService.getAllJobsStatus();
    }

    @PostMapping("/jobs")
    public ResponseEntity triggerMigration() {
        ResponseMessage responseMessage = new ResponseMessage();
        Set<String> alreadyTriggeredTables = dataMigrationService.triggerAllMigrationJobs();

        responseMessage.setStatus(Status.SUCCESS);
        String reponstMessage = TRIGGER_DATA_MIGRATION_SUCCESSFULLY;
        if (alreadyTriggeredTables.size() > 0) {
            reponstMessage += " Tables: " + alreadyTriggeredTables.toString()
                    + " migration will not be triggered, since there are other jobs running.";
        }
        responseMessage.setMessage(reponstMessage);

        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        ResponseMessage responseMessage = new ResponseMessage();
        if(dataMigrationService.isJobRunningOnTable(tableName)){
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Job can't be executed, currently another job is running for this table");
            return ResponseEntity.ok().body(responseMessage);
        } else {
            dataMigrationService.triggerOneMigrationJob(tableName);

            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage(TRIGGER_DATA_MIGRATION_SUCCESSFULLY);
            return ResponseEntity.ok().body(responseMessage);
        }
    }

    @PostMapping("/jobs/migrateSpecificRecords")
    public ResponseEntity<ResponseMessage> migrateSpecificRecords(@RequestBody List<MigrateRecord> migrateRecords) {

        dataMigrationService.migrateSpecificRecords(migrateRecords);
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage(TRIGGER_DATA_MIGRATION_SUCCESSFULLY);
        return ResponseEntity.ok().body(responseMessage);
    }
    @PostMapping("/data/cleanup/{tableName}")
    public ResponseEntity<ResponseMessage> dataCleanup4OneTable(@PathVariable final String tableName) {
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Data cleanup successfully done for the table: " + tableName + ".");
        dataCleanupService.cleanData4OneTable(tableName);

        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/data/cleanup")
    public ResponseEntity<ResponseMessage> dataCleanup4AllTables() {
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Data cleanup successfully done for all the tables.");
        dataCleanupService.cleanData4AllTables();
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
    
    @PostMapping("/data/verification")
    public ResponseEntity<ResponseMessage> dataVerificationForOneTable() throws ExecutionException, InterruptedException {
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForAllTable());
    }


    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<ResponseMessage> dataVerificationForAllTable(@PathVariable("tableName")final String tableName) throws ExecutionException, InterruptedException {
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForOneTable(tableName));
    }

    @PostMapping("/initialization")
    public ResponseEntity<ResponseMessage> tableInitializeAll() {
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Initialize successfully done for all the tables.");

        initializerService.initialize4AllTables();

        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/initialization/{tableName}")
    public ResponseEntity<ResponseMessage> tableInitializeOne(@PathVariable("tableName")final String tableName) {
        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        responseMessage.setMessage("Initialize successfully done for the table: " + tableName + ".");

        initializerService.initialize4OneTable(tableName);

        return ResponseEntity.ok().body(responseMessage);
    }

}
