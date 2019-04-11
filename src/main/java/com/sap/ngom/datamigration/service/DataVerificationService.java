package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.model.verification.*;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.DBHashSqlGenerator;
import com.sap.ngom.datamigration.util.TenantHelper;
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
import java.util.LinkedHashMap;

@Log4j2
@Service
public class DataVerificationService {

    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("targetDataSource")
    private DataSource targetDataSource;

    @Autowired
    private DBConfigReader dbConfigReader;

    @Autowired
    private TenantHelper tenantHelper;

    @Autowired
    private DBHashSqlGenerator dbHashSqlGenerator;

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
        log.info("Data verification is starting for table: " + tableName);

        TableResult tableResult = new TableResult();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        TableInfo tableInfo = new TableInfo();
        tableInfo.setSourceTableName(tableName);
        tableInfo.setTargetTableName(dbConfigReader.getTargetTableName(tableName));
        tableInfo.setTenantColumnName(tenantHelper.determineTenant(tableName));
        String retrievePrimaryKeySql = "select kc.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kc on kc.table_name = \'" + tableName + "\' and kc.table_schema = \'public\' and kc.constraint_name = tc.constraint_name where tc.constraint_type = \'PRIMARY KEY\'  and kc.ordinal_position is not null order by column_name";
        String sqlForTenantAndCount = "select count(" + tableInfo.getTenantColumnName() + ") as tenant_count, " + tableInfo.getTenantColumnName() + " from " + tableName + " where " + tableInfo.getTenantColumnName() + " is not null group by " + tableInfo.getTenantColumnName();
        List<String> tablePrimaryKeyList = jdbcTemplate.queryForList(retrievePrimaryKeySql,String.class);

        StringBuilder tablePrimaryKeyBuilder = new StringBuilder();
        if(tablePrimaryKeyList.isEmpty()) {
            log.warn("MD5 check would be skipped as the table " + tableName + "doesn't contain primary key.");
        } else{
            for(String primaryKeyField:tablePrimaryKeyList){
                tablePrimaryKeyBuilder.append(primaryKeyField).append("||\',\'||");
            }
            tableInfo.setPrimaryKey(tablePrimaryKeyBuilder.delete(tablePrimaryKeyBuilder.length()-7,tablePrimaryKeyBuilder.length()).toString());

        }

        Map<String,Integer> queryResult = jdbcTemplate.query(sqlForTenantAndCount, new ResultSetExtractor<Map<String,Integer>>() {
            @Override
            public Map<String,Integer> extractData(ResultSet resultSet) throws SQLException {
                Map map = new HashMap();
                while (resultSet.next()) {
                    map.put(resultSet.getString(tableInfo.getTenantColumnName()), resultSet.getInt("tenant_count"));

                }
                return map;
            }
        });

        List<TenantResult> tenantsResultList = new ArrayList<>();
        tableResult.setDataConsistent(true);
        boolean testttt = true;
        for (String tenant : queryResult.keySet()) {
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            int targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + tableInfo.getTargetTableName() + "\"",Integer.class);
            CountResult countResult = new CountResult();
            TenantResult tenantResult = new TenantResult();
            boolean tenantDataConsistent = true;
            if(targetTenantCount != queryResult.get(tenant)){
                tableResult.setDataConsistent(false);
                tenantDataConsistent = false;

            } else if(targetTenantCount != 0 && !tablePrimaryKeyList.isEmpty()){
                 /*
                  *  MD5 content check
                  */
                log.info("Hash consistent check is starting for tenant (" + tenant + ") in table: " + tableName);
                tableInfo.setTenant(tenant);
                String hana_md5_sql = dbHashSqlGenerator.generateHanaMd5Sql(tableInfo,hanaJdbcTemplate);

                Map<String,String> hanaMd5Result = hanaJdbcTemplate.query(hana_md5_sql, new ResultSetExtractor<Map<String,String>>() {
                    @Override
                    public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                        Map map = new HashMap();
                        while (resultSet.next()) {
                            map.put(resultSet.getString("tablePrimaryKey"), resultSet.getString("md5Result"));

                        }
                        return map;
                    }
                });



              //  List<String> hana_md5_list =hanaJdbcTemplate.queryForList(hana_md5_sql,String.class);
                JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);

                String postgres_md5_sql = dbHashSqlGenerator.generatePostgresMd5Sql(tableInfo, postgresJdbcTemplate);
                Map<String,String> postgresMd5Result = jdbcTemplate.query(postgres_md5_sql, new ResultSetExtractor<Map<String,String>>() {
                    @Override
                    public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                        Map map = new HashMap();
                        while (resultSet.next()) {
                            map.put(resultSet.getString("tablePrimaryKey"), resultSet.getString("md5Result"));

                        }
                        return map;
                    }
                });

                if(testttt) {
                    log.info("postgres_sql: " + postgres_md5_sql);
                    log.info("hana_sql: " + hana_md5_sql);
                    testttt = false;
                }
                List<String> failedRecords = new ArrayList<>();
                for(String primaryKeyValue: postgresMd5Result.keySet()){
                    if(!hanaMd5Result.containsKey(primaryKeyValue) || !hanaMd5Result.get(primaryKeyValue).equals(postgresMd5Result.get(primaryKeyValue))){
                        tableResult.setDataConsistent(false);
                        tenantDataConsistent = false;
                        failedRecords.add(primaryKeyValue);

                    }
                }
                tenantResult.setInconsistentRecords(failedRecords);

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
        if(!tableResult.getDataConsistent()){
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
            if(!tablePrimaryKeyList.isEmpty()){
                tableResult.setPrimaryKey(tableInfo.getPrimaryKey().replace("||\',\'||",","));
            }
        }

        log.info("Data verification is completed for table: " + tableName);
        return tableResult;
    }
}
