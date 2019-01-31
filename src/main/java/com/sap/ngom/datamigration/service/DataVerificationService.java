package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.model.verification.CountResult;
import com.sap.ngom.datamigration.model.verification.Detail;
import com.sap.ngom.datamigration.model.verification.TableResult;
import com.sap.ngom.datamigration.model.verification.TenantResult;
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

    public ResponseMessage dataVerificationForOneTable(String tableName) {

        ResponseMessage responseMessage = new ResponseMessage();
        TableResult tableResult = verifyOneTableResult(tableName);

        if (tableResult.getTenantVaildFlag()) {
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);
        } else {
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Data INCONSISTENT between source and target after verification.");

            List<TableResult> tablesResultList = new ArrayList<>();
            tableResult.setTenantVaildFlag(null);
            tablesResultList.add(tableResult);
            Detail detail = new Detail();
            detail.setTables(tablesResultList);
            responseMessage.setDetail(detail);
        }

        return responseMessage;
    }

    public ResponseMessage dataVerificationForAllTable() {
        List<String> tableList = dbConfigReader.getSourceTableNames();

        ResponseMessage responseMessage = new ResponseMessage();
        Detail detail = new Detail();
        List<TableResult> tablesResultList = new ArrayList<>();

        detail.setTableValidFlag(true);
        for (String tableName : tableList) {
            TableResult tableResult = verifyOneTableResult(tableName);
            if(!tableResult.getTenantVaildFlag()){
                detail.setTableValidFlag(false);
                tableResult.setTenantVaildFlag(null);
                tablesResultList.add(tableResult);
            }

        }

        if(detail.getTableValidFlag()){
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);

        } else{
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Data INCONSISTENT between source and target after verification.");
            detail.setTableValidFlag(null);
            detail.setTables(tablesResultList);
            responseMessage.setDetail(detail);
        }

        return responseMessage;
    }



    private TableResult verifyOneTableResult(String tableName) {

        TableResult tableResult = new TableResult();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        String targetTableName = dbConfigReader.getTargetTableName(tableName);
        int targetTenantCount;
        String sqlForTenantAndCount = "select count(tenant_id) as tenant_count, tenant_id from " + tableName + " group by tenant_id";

        log.info("Data verification is starting for table: " + tableName);
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
        tableResult.setTenantVaildFlag(true);
        for(String tenant : queryResult.keySet()){
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + targetTableName + "\"",Integer.class);
            if(targetTenantCount != queryResult.get(tenant)){
                tableResult.setTenantVaildFlag(false);
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

        if(!tableResult.getTenantVaildFlag()){
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
        }

        return tableResult;
    }
}
