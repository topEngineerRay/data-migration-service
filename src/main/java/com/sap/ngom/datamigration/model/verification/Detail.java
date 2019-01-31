package com.sap.ngom.datamigration.model.verification;

import lombok.*;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Detail {
    private List<TableResult> tables;
}
