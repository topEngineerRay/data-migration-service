package com.sap.ngom.datamigration.model.verification;

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
