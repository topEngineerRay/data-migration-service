package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TenantHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


@Service
public class DataVerificationService {

    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("targetDataSource")
    DataSource targetDataSource;

    @Autowired
    DBConfigReader dbConfigReader;

    public DataVerificationResult tableMigrationResultVerification(String tableName){
        DataVerificationResult dataVerificationResult = new DataVerificationResult();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        String targetTableName = dbConfigReader.getTargetTableName(tableName);
        boolean countIsMatch = true;
        int targetTenantCount;

        String sqlForTenantAndCount = "select count(tenant_id) as tenant_count, tenant_id from " + tableName + " group by tenant_id";
        Map<String,Integer> queryResult = jdbcTemplate.query(sqlForTenantAndCount, new ResultSetExtractor<Map<String,Integer>>() {
            @Override
            public Map<String,Integer> extractData(ResultSet resultSet) throws SQLException {
                Map map = new HashMap();
                while (resultSet.next()) {
                    map.put(resultSet.getString("tenant_id"), resultSet.getInt("tenant_count"));

                }
                return map;
        }
        });


        Map tenantResultMap = new HashMap();
        for(String tenant:queryResult.keySet()){
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + targetTableName + "\"",Integer.class);
            if(targetTenantCount != queryResult.get(tenant)){
                countIsMatch = false;
                TenantResult tenantResult = new TenantResult();
                tenantResult.setSourceCount(queryResult.get(tenant));
                tenantResult.setTargetCount(targetTenantCount);
                tenantResultMap.put(tenant,tenantResult);
            }
        }


        if(countIsMatch){
            dataVerificationResult.setStatus(Status.SUCCESSFUL);
            dataVerificationResult.setMessage("Data CONSISTENT between source and target after verification.");
            dataVerificationResult.setDetails(null);

            return dataVerificationResult;
        }else{
            dataVerificationResult.setStatus(Status.FAILED);
            dataVerificationResult.setMessage("Data INCONSISTENT between source and target after verification.");
            TableResult tableResult = new TableResult();
            tableResult.setTenants(tenantResultMap);
            Map tableResultMap = new HashMap();
            tableResultMap.put(tableName,tableResult);

            Detail detail = new Detail();
            detail.setTables(tableResultMap);
            dataVerificationResult.setDetails(detail);

            return dataVerificationResult;
        }
    }



}
