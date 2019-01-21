package com.sap.ngom.datamigration.model;

import lombok.*;
import org.springframework.batch.core.ExitStatus;
import org.springframework.stereotype.Component;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Component
public class JobResult {
    private long jobId;
    private String jobName;
    private ExitStatus jobExitStatus;
    private long timestamp;
}