package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.exception.JobAlreadyRuningException;
import com.sap.ngom.datamigration.model.JobStatus;
import com.sap.ngom.datamigration.model.ResponseMessage;
import com.sap.ngom.datamigration.model.Status;
import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import com.sap.ngom.datamigration.service.DataVerificationService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

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
    DataVerificationService dataVerificationService;

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
   

    @PostMapping("/jobs")
    public ResponseEntity triggerMigration()
    {

        Set<String> alreadyTriggeredTables = dataMigrationService.triggerAllMigrationJobs();

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setStatus(Status.SUCCESS);
        String reponstMessage = TRIGGER_DATA_MIGRATION_SUCCESSFULLY;
        if(alreadyTriggeredTables.size()>0){
            reponstMessage = "Tables: " + alreadyTriggeredTables.toString() + " migration will not be triggered, since there are other jobs running.";
        }
        responseMessage.setMessage(reponstMessage);
        return ResponseEntity.ok().body(responseMessage);
    }

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        ResponseMessage responseMessage = new ResponseMessage();
        try{
            dataMigrationService.triggerOneMigrationJob(tableName);
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage(TRIGGER_DATA_MIGRATION_SUCCESSFULLY);
            return ResponseEntity.ok().body(responseMessage);
        }catch(JobAlreadyRuningException e){
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage(e.getMessage());
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

    @PostMapping("/data/verification")
    public ResponseEntity<ResponseMessage> dataVerificationForOneTable(){
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForAllTable());
    }


    @PostMapping("/data/verification/{tableName}")
    public ResponseEntity<ResponseMessage> dataVerificationForAllTable(@PathVariable("tableName")final String tableName){
        return ResponseEntity.status(200).body(dataVerificationService.dataVerificationForOneTable(tableName));
    }

}
