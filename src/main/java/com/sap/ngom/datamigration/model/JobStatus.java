package com.sap.ngom.datamigration.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.util.Date;

@Builder
@Data
public class JobStatus {
    private String table;
    private String jobStatus;
    private Date jobStartTime;
    private Date jobEndTime;
}
