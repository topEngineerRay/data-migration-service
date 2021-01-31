package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.exception.SourceTableNotDefinedException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

@Component
public class DBConfigReader {

    @Value("${data.migration.db.source.tables}")
    private String[] sourceTableNames;

    @Value("${data.migration.db.target.namespace}")
    private String targetTableNameSpace;

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
            throw new SourceTableNotDefinedException("The given source table name (" + sourceTableName + ") is not defined in properties.");
        }
        return isNameSpaceSpecified() ? targetTableNameSpace + "." + sourceTableName : sourceTableName;
    }

    public String getTargetNameSpace(){
        return targetTableNameSpace;
    }
    private boolean isNameSpaceSpecified() {
        return targetTableNameSpace != null && !targetTableNameSpace.isEmpty();
    }

}
