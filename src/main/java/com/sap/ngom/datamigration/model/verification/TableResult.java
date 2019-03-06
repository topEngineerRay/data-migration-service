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
public class TableResult {
    private String table;
    private Boolean dataConsistent;
    private String primaryKey;
    private List<TenantResult> tenants;
}
