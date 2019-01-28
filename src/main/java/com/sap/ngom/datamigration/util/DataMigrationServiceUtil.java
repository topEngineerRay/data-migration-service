package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.exception.DataMigrationProcessException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class DataMigrationServiceUtil {

    @Value("${migration.table.target.nameSpace}")
    private String targetTableNameSpace;
    @Value("${migration.table.source.names}")
    private String[] sourceTableNames;

    private List<String> sourceTableNamesList;

    @PostConstruct
    public void createSourceTableNamesList(){
        this.sourceTableNamesList = Arrays.asList(sourceTableNames);
    }

    public List<String> getSourceTableNames(){
        return sourceTableNamesList;
    }

    public String getTargetTableName (String sourceTableName){
        if(!sourceTableNamesList.contains(sourceTableName)){
            throw new DataMigrationProcessException("The given source table name (" + sourceTableName + ") is not defined in properties.");
        }
        return isNameSpaceUsed() ? targetTableNameSpace + "." + sourceTableName : sourceTableName;
    }

    private boolean isNameSpaceUsed () {
        return targetTableNameSpace == null || !targetTableNameSpace.isEmpty();
    }

}
