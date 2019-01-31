package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.model.dataVerification.CountResult;
import com.sap.ngom.datamigration.model.dataVerification.Detail;
import com.sap.ngom.datamigration.model.dataVerification.TableResult;
import com.sap.ngom.datamigration.model.dataVerification.TenantResult;
import com.sap.ngom.datamigration.util.DBConfigReader;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
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

    public ResponseMessage tableMigrationResultVerification(String tableName){
        ResponseMessage responseMessage = new ResponseMessage();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        String targetTableName = dbConfigReader.getTargetTableName(tableName);
        boolean countIsMatch = true;
        int targetTenantCount;
        log.info("Data verification is starting for table: " + tableName);
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


        List<TenantResult> tenantsResultList = new ArrayList<>();

        for(String tenant : queryResult.keySet()){
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + targetTableName + "\"",Integer.class);
            if(targetTenantCount != queryResult.get(tenant)){
                countIsMatch = false;
                CountResult countResult = new CountResult();
                countResult.setSourceCount(queryResult.get(tenant));
                countResult.setTargetCount(targetTenantCount);

                TenantResult tenantResult = new TenantResult();
                tenantResult.setTenant(tenant);
                tenantResult.setCountResult(countResult);

                tenantsResultList.add(tenantResult);
            }
            log.info("Data verification is completed for tenant(" + tenant + ") in table: " + tableName);
        }

        log.info("Data verification is completed for table: " + tableName);

        if(countIsMatch){
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);
            return responseMessage;

        } else{
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Data INCONSISTENT between source and target after verification.");

            TableResult tableResult = new TableResult();
            List<TableResult> tablesResultList = new ArrayList<>();
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
            tablesResultList.add(tableResult);

            Detail detail = new Detail();
            detail.setTables(tablesResultList);
            responseMessage.setDetail(detail);

            return responseMessage;
        }
    }
}
