package com.sap.ngom.datamigration.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

@Builder
@Data
public class JobStatus {
    private String table;
    private BatchStatus jobStatus;
}
