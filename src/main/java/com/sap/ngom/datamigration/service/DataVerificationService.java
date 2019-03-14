package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.model.verification.CountResult;
import com.sap.ngom.datamigration.model.verification.Detail;
import com.sap.ngom.datamigration.model.verification.TableResult;
import com.sap.ngom.datamigration.model.verification.TenantResult;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.DBHashSqlGenerator;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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

    @Autowired
    DBHashSqlGenerator dbHashSqlGenerator;

    public ResponseMessage dataVerificationForOneTable(String tableName) {

        ResponseMessage responseMessage = new ResponseMessage();
        TableResult tableResult = verifyOneTableResult(tableName);

        if (tableResult.getDataConsistent()) {
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);
        } else {
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Data INCONSISTENT between source and target after verification.");

            List<TableResult> tablesResultList = new ArrayList<>();
            tablesResultList.add(tableResult);
            Detail detail = new Detail();
            detail.setTables(tablesResultList);
            responseMessage.setDetail(detail);
        }

        return responseMessage;
    }

    public ResponseMessage dataVerificationForAllTable() {

        boolean isTableDataConsistent = true;
        List<String> tableList = dbConfigReader.getSourceTableNames();
        ResponseMessage responseMessage = new ResponseMessage();
        List<TableResult> tablesResultList = new ArrayList<>();

        for (String tableName : tableList) {
            TableResult tableResult = verifyOneTableResult(tableName);
            if(!tableResult.getDataConsistent()){
                isTableDataConsistent = false;
                tablesResultList.add(tableResult);
            }

        }

        if(isTableDataConsistent){
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);
        } else{
            responseMessage.setStatus(Status.FAILURE);
            responseMessage.setMessage("Data INCONSISTENT between source and target after verification.");

            Detail detail = new Detail();
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
        String retreivePrimaryKeySql = "select kc.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kc on kc.table_name = \'" + tableName + "\' and kc.table_schema = \'public\' and kc.constraint_name = tc.constraint_name where tc.constraint_type = \'PRIMARY KEY\'  and kc.ordinal_position is not null order by column_name";

        log.info("Data verification is starting for table: " + tableName);
        List<String> tablePrimaryKeyList = jdbcTemplate.queryForList(retreivePrimaryKeySql,String.class);
        String tablePrimaryKey = "";
        if(tablePrimaryKeyList.isEmpty()) {
            log.warn("MD5 check would be skipped as the table " + tableName + "doesn't contain primary key.");
        } else{
            StringBuilder tablePrimaryKeyBuilder = new StringBuilder();
            for(String primaryKeyField:tablePrimaryKeyList){
                tablePrimaryKeyBuilder.append(primaryKeyField).append(",");
            }
            tablePrimaryKey = tablePrimaryKeyBuilder.delete(tablePrimaryKeyBuilder.length()-1,tablePrimaryKeyBuilder.length()).toString();

        }

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
        tableResult.setDataConsistent(true);
        for(String tenant : queryResult.keySet()){
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + targetTableName + "\"",Integer.class);
            CountResult countResult = new CountResult();
            TenantResult tenantResult = new TenantResult();
            boolean tenantDataConsistent = true;
            if(targetTenantCount != queryResult.get(tenant)){
                tableResult.setDataConsistent(false);
                tenantDataConsistent = false;

            } else if(targetTenantCount != 0 && !tablePrimaryKeyList.isEmpty()){
                log.info("Hash consistent check is starting for tenant (" + tenant + ") in table: " + tableName);
                String hana_md5_sql = dbHashSqlGenerator.generateHanaMd5Sql(targetTableName,hanaJdbcTemplate,tablePrimaryKey);

                List<String> hana_md5_list =hanaJdbcTemplate.queryForList(hana_md5_sql,String.class);
                JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);

                String postgres_md5_sql = dbHashSqlGenerator.generatePostgresMd5Sql(tableName,tenant,postgresJdbcTemplate,tablePrimaryKey);
                Map<String,String> postgresMd5Result = jdbcTemplate.query(postgres_md5_sql, new ResultSetExtractor<Map<String,String>>() {
                    @Override
                    public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                        Map map = new LinkedHashMap();
                        while (resultSet.next()) {
                            map.put(resultSet.getString("md5Result"), resultSet.getString("tablePrimaryKey"));

                        }
                        return map;
                    }
                });

                int index = 0;
                List<String> failedRecords = new ArrayList<>();
                for(String md5Result: postgresMd5Result.keySet()){
                    if(!md5Result.equals(hana_md5_list.get(index))){
                        tableResult.setDataConsistent(false);
                        tenantDataConsistent = false;
                        failedRecords.add(postgresMd5Result.get(md5Result));
                    }
                    index++;
                }
                tenantResult.setInconsistentRecordsResult(failedRecords);

            }

            if(!tenantDataConsistent){
                countResult.setSourceCount(queryResult.get(tenant));
                countResult.setTargetCount(targetTenantCount);
                tenantResult.setTenant(tenant);
                tenantResult.setCountResult(countResult);
                tenantsResultList.add(tenantResult);
            }

            log.info("Data verification is completed for tenant (" + tenant + ") in table: " + tableName);
        }

        log.info("Data verification is completed for table: " + tableName);

        if(!tableResult.getDataConsistent()){
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
            tableResult.setPrimaryKey(tablePrimaryKey);
        }
        return tableResult;
    }
}
