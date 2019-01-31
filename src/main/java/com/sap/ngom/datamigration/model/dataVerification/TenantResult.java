package com.sap.ngom.datamigration.model.dataVerification;

import com.sap.ngom.datamigration.model.dataVerification.CountResult;
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
