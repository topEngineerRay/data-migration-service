package com.sap.ngom.datamigration.listener;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class BPStepListener implements StepExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private String tenantId;
    public BPStepListener(String tenantId){
        this.tenantId = tenantId;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        TenantThreadLocalHolder.setTenant(this.tenantId);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("The migration step:" + stepExecution.getStepName() + "start at:" + stepExecution.getStartTime()
                + "end at:" + stepExecution.getEndTime());

        log.info("data count after read of this step:" +stepExecution.getReadCount());
        log.info("data commit count after read:" +stepExecution.getCommitCount());
        log.info("data write count after read:" +stepExecution.getWriteCount());

        return stepExecution.getExitStatus();
    }
}
