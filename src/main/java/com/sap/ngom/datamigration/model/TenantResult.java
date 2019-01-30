package com.sap.ngom.datamigration.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TenantResult {
    private int sourceCount;
    private int targetCount;
}
