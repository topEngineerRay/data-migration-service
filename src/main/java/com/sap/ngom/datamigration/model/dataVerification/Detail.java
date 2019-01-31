package com.sap.ngom.datamigration.model.dataVerification;

import com.sap.ngom.datamigration.model.dataVerification.TableResult;
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
