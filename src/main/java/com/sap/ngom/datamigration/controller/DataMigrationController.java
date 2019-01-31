package com.sap.ngom.datamigration.controller;

import com.sap.ngom.datamigration.service.DataCleanupService;
import com.sap.ngom.datamigration.service.DataMigrationService;
import com.sap.ngom.datamigration.service.ManagedInstanceService;
import com.sap.ngom.datamigration.util.ResponseMessage;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
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

    @Autowired
    ManagedInstanceService managedInstanceService;

    @PostMapping("/jobs/{tableName}")
    public ResponseEntity triggerTableMigration(@PathVariable("tableName")final String tableName) {
        return dataMigrationService.triggerOneMigrationJob(tableName);
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

    @PostMapping("/managed-instances/cleanup")
    public ResponseEntity<Void> managedInstancesClear() {
        try {
            managedInstanceService.deleteAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().build();
    }
}
