package com.sap.ngom.datamigration.model;

import lombok.Data;

@Data
public class MigrateRecord {
    public String tableName;
    public String tenant;
    public String primaryKeyName;
    public String primaryKeyValue;
}
