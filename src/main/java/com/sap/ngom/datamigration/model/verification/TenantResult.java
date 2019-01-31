package com.sap.ngom.datamigration.model.verification;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TenantResult {
    private String tenant;
    private CountResult countResult;
}
