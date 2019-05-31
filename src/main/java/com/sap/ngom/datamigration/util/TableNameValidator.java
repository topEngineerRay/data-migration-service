package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableNameValidator {
    @Autowired
    private DBConfigReader dbConfigReader;

    public void tableNameValidation(String tableName) {
        if (!dbConfigReader.getSourceTableNames().contains(tableName)) {
            throw new SourceTableNotDefinedException("The given source table name (" + tableName + ") is not defined in properties.");
        }
    }
}
