package com.sap.ngom.datamigration.configuration;

import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TableNameValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class BatchJobParameterHolder {

    @Autowired
    private DBConfigReader dbConfigReader;

    @Autowired
    private TableNameValidator tableNameValidator;

    private Map<String,Integer> jobParameterHolder = new HashMap<>();

     private Map<String, Boolean> jobLockHolder = new HashMap<>();

    @PostConstruct
    private void setInitialJobParameters(){
        dbConfigReader.getSourceTableNames().forEach(table -> {
            jobParameterHolder.put(table,0);
            jobLockHolder.put(table, false);
        });
    }

    public Integer getIncreasingParameter(String table){
        Integer original = jobParameterHolder.get(table);
        Integer increased = ++original;
        jobParameterHolder.put(table, increased);
        return increased;
    }

    public Integer getCurrentParameter(String table){
        return jobParameterHolder.get(table);
    }

    public synchronized boolean acquireJobLock(String table) {
        tableNameValidator.tableNameValidation(table);

        if(jobLockHolder.get(table)){
            return true;
        }else{
            jobLockHolder.put(table, true);
            return false;
        }
    }

    public void unLockJob(String table){
        jobLockHolder.put(table, false);
    }
}
