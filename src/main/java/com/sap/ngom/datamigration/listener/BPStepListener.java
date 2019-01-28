package com.sap.ngom.datamigration.listener;

import com.sap.ngom.datamigration.configuration.hana.TenantSpecificHANAMultitRoutingDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

//@Component("BPStepListener")
public class BPStepListener implements StepExecutionListener {
    private String tenantId;
    public BPStepListener(String tenantId){
        this.tenantId = tenantId;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        TenantSpecificHANAMultitRoutingDataSource
                .setTenant(this.tenantId);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}
