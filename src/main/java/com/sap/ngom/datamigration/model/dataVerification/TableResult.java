package com.sap.ngom.datamigration.model.dataVerification;


import lombok.*;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableResult {
    private String table;
    private List<TenantResult> tenants;
}
