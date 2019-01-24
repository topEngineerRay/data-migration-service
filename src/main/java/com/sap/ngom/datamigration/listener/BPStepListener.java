package com.sap.ngom.datamigration.listener;

import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.TenantSpecificHANAMultitRoutingDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

//@Component("BPStepListener")
public class BPStepListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        TenantSpecificHANAMultitRoutingDataSource
                .setTenant(TenantSpecificHANAMultitRoutingDataSource.getTenant(stepExecution.getStepName()));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}
