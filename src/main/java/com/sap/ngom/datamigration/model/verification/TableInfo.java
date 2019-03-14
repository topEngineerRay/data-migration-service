package com.sap.ngom.datamigration.model.verification;


import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableInfo {
    private String sourceTableName;
    private String targetTableName;
    private String tenantColumnName;
    private String tenant;
    private String primaryKey;
}
