package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.model.ResponseMessage;
import com.sap.ngom.datamigration.model.Status;
import com.sap.ngom.datamigration.service.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

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
    public ResponseEntity triggerMigration()
    {

        Set<String> alreadyTriggeredTables = dataMigrationService.triggerAllMigrationJobs();

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        String reponstMessage = TRIGGER_DATA_MIGRATION_SUCCESSFULLY;
        if(alreadyTriggeredTables.size()>0){
            reponstMessage += " Tables: " + alreadyTriggeredTables.toString() + " migration will not be triggered, since there are other jobs running.";
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
        }else{
            dataMigrationService.triggerOneMigrationJob(tableName);
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage(TRIGGER_DATA_MIGRATION_SUCCESSFULLY);
            return ResponseEntity.ok().body(responseMessage);
        }
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
    
    @PostMapping("/data/verification")
    public ResponseEntity<ResponseMessage> dataVerificationForOneTable(){
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForAllTable());
    }


    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<ResponseMessage> dataVerificationForAllTable(@PathVariable("tableName")final String tableName){
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForOneTable(tableName));
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
