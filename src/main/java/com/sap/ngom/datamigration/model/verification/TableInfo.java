package com.sap.ngom.datamigration.model.verification;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
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
    private String primaryKey;
    private String originalPrimaryKey;
    private String postgresMd5Sql;
    public String getTenant(){
        return TenantThreadLocalHolder.getTenant();
    }
}
