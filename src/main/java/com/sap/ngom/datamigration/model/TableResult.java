package com.sap.ngom.datamigration.model;


import lombok.*;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableResult {
    private Map<String,TenantResult> tenants;
}
