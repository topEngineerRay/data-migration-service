package com.sap.ngom.datamigration.model.dataVerification;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CountResult {
    private int sourceCount;
    private int targetCount;
}
