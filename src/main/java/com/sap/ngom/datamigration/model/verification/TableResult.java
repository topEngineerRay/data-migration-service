package com.sap.ngom.datamigration.model.verification;


import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableResult {
    private String table;
    private AtomicBoolean dataConsistent;
    private String primaryKey;
    private List<TenantResult> tenants;
}
