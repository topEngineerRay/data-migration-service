package com.sap.ngom.datamigration.configuration;

import com.sap.ngom.datamigration.util.DBConfigReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchJobParameterHolder {

    @Autowired
    private DBConfigReader dbConfigReader;

    private Map<String,Integer> jobParameterHolder = new HashMap<>();

    @PostConstruct
    private void setInitialJobParameters(){
        dbConfigReader.getSourceTableNames().forEach(table -> {
            jobParameterHolder.put(table,0);
        });
    }

    public Integer getParameter(String table){
        return jobParameterHolder.get(table);
    }

    public void setParameter(String table, Integer value){
        jobParameterHolder.put(table,value);
    }
}
