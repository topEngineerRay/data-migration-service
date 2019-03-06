package com.sap.ngom.datamigration.model.verification;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TenantResult {
    private String tenant;
    private CountResult countResult;
    private List<String> inconsistentRecordsResult;
}
