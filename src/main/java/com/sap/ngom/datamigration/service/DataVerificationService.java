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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

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

    private static final int MISMATCH_RECORDS_MAX_NUM = 100;
    private static final String MORE_INDICATOR = "..more";
    private static final String PK_DELIMITER = "||\',\'||";



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
        String sqlForTenantAndCount = "select count(" + tableInfo.getTenantColumnName() + ") as tenant_count, " + tableInfo.getTenantColumnName() + " from " + tableName + " where " + tableInfo.getTenantColumnName() + " is not null group by " + tableInfo.getTenantColumnName();

        List<String> tablePrimaryKeyList = getPrimaryKeysByTable(tableName, jdbcTemplate);

        StringBuilder tablePrimaryKeyBuilder = new StringBuilder();
        if(tablePrimaryKeyList.isEmpty()) {
            log.warn("MD5 check would be skipped as the table " + tableName + "doesn't contain primary key.");
        } else{
            for(String primaryKeyField:tablePrimaryKeyList){
                tablePrimaryKeyBuilder.append(primaryKeyField).append(PK_DELIMITER);
            }
            tableInfo.setPrimaryKey(tablePrimaryKeyBuilder.delete(tablePrimaryKeyBuilder.length()-7,tablePrimaryKeyBuilder.length()).toString());

        }

        Map<String,Integer> tenantAndCountMap = jdbcTemplate.query(sqlForTenantAndCount, resultSet -> {
            Map<String, Integer> map = new HashMap<>();
            while (resultSet.next()) {
                map.put(resultSet.getString(tableInfo.getTenantColumnName()), resultSet.getInt("tenant_count"));

            }
            return map;
        });

        List<TenantResult> tenantsResultList = new ArrayList<>();
        tableResult.setDataConsistent(true);
        for (String tenant : tenantAndCountMap.keySet()) {
            TenantThreadLocalHolder.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            int targetTenantCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + tableInfo.getTargetTableName() + "\"",Integer.class);
            CountResult countResult = new CountResult();
            TenantResult tenantResult = new TenantResult();
            boolean tenantDataConsistent = true;
            if(targetTenantCount != tenantAndCountMap.get(tenant)){
                tableResult.setDataConsistent(false);
                tenantDataConsistent = false;

            } else if(targetTenantCount != 0 && !tablePrimaryKeyList.isEmpty()){
                 /*
                  *  MD5 content check
                  */
                log.info("Hash consistent check is starting for tenant (" + tenant + ") in table: " + tableName);
                tableInfo.setTenant(tenant);
                String hana_md5_sql = dbHashSqlGenerator.generateHanaMd5Sql(tableInfo,hanaJdbcTemplate);
                Map<String,MD5Result> hanaMd5Result = hanaJdbcTemplate.query(hana_md5_sql, resultSet -> {
                    Map<String, MD5Result> map = new HashMap<>();
                    while (resultSet.next()) {
                        MD5Result md5Result = new MD5Result();
                        md5Result.setMd5Value(resultSet.getString("md5Result"));
                        map.put(resultSet.getString("tablePrimaryKey"), md5Result);

                    }
                    return map;
                });


                JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);

                  String postgres_md5_sql = dbHashSqlGenerator.generatePostgresMd5Sql(tableInfo, postgresJdbcTemplate);
                Map<String,MD5Result> postgresMd5Result = jdbcTemplate.query(postgres_md5_sql, new ResultSetExtractor<Map<String,MD5Result>>() {
                    @Override
                    public Map<String,MD5Result> extractData(ResultSet resultSet) throws SQLException {
                        Map<String, MD5Result> map = new HashMap<>();
                        while (resultSet.next()) {
                            MD5Result md5Result = new MD5Result();
                            md5Result.setMd5Value(resultSet.getString("md5Result"));
                            map.put(resultSet.getString("tablePrimaryKey"), md5Result);

                        }
                        return map;
                    }
                });

                List<String> failedRecords = new ArrayList<>();

                Map<String, MD5Result> failedRecordMap = new HashMap<>();

                for(Map.Entry<String, MD5Result> entry : postgresMd5Result.entrySet()){
                    if(!hanaMd5Result.containsKey(entry.getKey())) {
                        entry.getValue().setVerifiedStatus(MD5Result.VerifiedStatus.INCONSISTENT);
                        failedRecordMap.put(entry.getKey(),entry.getValue());
                    } else if(!hanaMd5Result.get(entry.getKey()).getMd5Value().equals(postgresMd5Result.get(entry.getKey()).getMd5Value())) {

                        entry.getValue().setVerifiedStatus(MD5Result.VerifiedStatus.CHECKAGAIN);
                        failedRecordMap.put(entry.getKey(),entry.getValue());
                    }

                    if(failedRecordMap.size() == MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                        Map<String, MD5Result> needDoubleCheckFailedRecords = failedRecordMap.entrySet().stream()
                                                                                .filter( x -> x.getValue().getVerifiedStatus().equals(MD5Result.VerifiedStatus.CHECKAGAIN))
                                                                                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                        checkRecordsIfActualEqual(needDoubleCheckFailedRecords, tableInfo);
                    }
                    if(failedRecordMap.size() == MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                        failedRecords.add(MORE_INDICATOR);
                        tenantResult.setInconsistentRecords(failedRecords);
                        log.warn("Data verification only checked the first " + MISMATCH_RECORDS_MAX_NUM + " records for tenant (" + tenant + ") in table: " + tableName +", as the inconsistent records reaches the predefined max number.");
                        break;
                    }
                }
                Map<String, MD5Result> needDoubleCheckFailedRecords = failedRecordMap.entrySet().stream()
                        .filter( x -> x.getValue().getVerifiedStatus().equals(MD5Result.VerifiedStatus.CHECKAGAIN))
                        .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

                checkRecordsIfActualEqual(needDoubleCheckFailedRecords, tableInfo);
                failedRecords = failedRecordMap.entrySet().stream()
                                .filter(x -> x.getValue().getVerifiedStatus().equals(MD5Result.VerifiedStatus.INCONSISTENT))
                                .map( x -> x.getKey()).collect(Collectors.toList());

                if(!failedRecords.isEmpty()) {
                    tableResult.setDataConsistent(false);
                    tenantDataConsistent = false;
                    tenantResult.setInconsistentRecords(failedRecords);
                }
            }

            if(!tenantDataConsistent){
                countResult.setSourceCount(tenantAndCountMap.get(tenant));
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
                tableResult.setPrimaryKey(tableInfo.getPrimaryKey().replace(PK_DELIMITER,","));
            }
        }

        log.info("Data verification is completed for table: " + tableName);
        return tableResult;
    }

    public List<String> getPrimaryKeysByTable(String tableName, JdbcTemplate jdbcTemplate) {
        String retrievePrimaryKeySql =
                "select kc.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kc on kc.table_name = \'"
                        + tableName
                        + "\' and kc.table_schema = \'public\' and kc.constraint_name = tc.constraint_name where tc.constraint_type = \'PRIMARY KEY\'  and kc.ordinal_position is not null order by column_name";
        return jdbcTemplate.queryForList(retrievePrimaryKeySql, String.class);
    }





    private void checkRecordsIfActualEqual(Map<String, MD5Result> failedRecordMap, TableInfo tableInfo) {
        if(failedRecordMap.isEmpty()){
            return;
        }

        JdbcTemplate hanaJdbcTemplateQ = new JdbcTemplate(targetDataSource);
        JdbcTemplate postgresJdbcTemplateQ = new JdbcTemplate(sourceDataSource);

        String selectAllSqlBasicPosgres = dbHashSqlGenerator.generateSortedSelectAllSqlPostgres(tableInfo, postgresJdbcTemplateQ);
        String selectAllSqlBasicHANA = dbHashSqlGenerator.generateSortedSelectAllSqlHANA(tableInfo,hanaJdbcTemplateQ);
        String whereStatement = dbHashSqlGenerator.generateWhereStatFindSpecificPKSql(tableInfo.getPrimaryKey().replace(PK_DELIMITER, ","),failedRecordMap.keySet());
        String selectAllSqlPosgres = selectAllSqlBasicPosgres + whereStatement ;
        String selecAllSqlHANA = selectAllSqlBasicHANA + whereStatement;

        Map<String,List<String>> getOneRowPostgres = postgresJdbcTemplateQ.query(selectAllSqlPosgres, new ResultSetExtractor<Map<String,List<String>>>() {
            @Override
            public Map<String,List<String>> extractData(ResultSet resultSet) throws SQLException {
                final ResultSetMetaData metaData = resultSet.getMetaData();

                final int columnCount = metaData.getColumnCount();
                final Map<String,List<String>> recordList = new HashMap<>();


                while(resultSet.next()){
                    final List<String> fieldValues = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        if (resultSet.getString(i) != null) {
                            fieldValues.add(resultSet.getString(i));
                        }
                    }
                    recordList.put(resultSet.getString("tablePrimaryKey"),fieldValues);

                }
                return recordList;
            }
        });

        Map<String,List<String>> getOneRowHANA = hanaJdbcTemplateQ.query(selecAllSqlHANA, new ResultSetExtractor<Map<String,List<String>>>() {
            @Override
            public Map<String,List<String>> extractData(ResultSet resultSet) throws SQLException {
                final ResultSetMetaData metaData = resultSet.getMetaData();

                final int columnCount = metaData.getColumnCount();
                final Map<String,List<String>> recordList = new HashMap<>();


                while(resultSet.next()){
                    final List<String> fieldValues = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        if (resultSet.getString(i) != null) {
                            fieldValues.add(resultSet.getString(i));
                        }
                    }
                    recordList.put(resultSet.getString("tablePrimaryKey"),fieldValues);


                }
                return recordList;
            }
        });

        for(Map.Entry<String, List<String>> entry : getOneRowPostgres.entrySet()) {
            if(!entry.getValue().equals(getOneRowHANA.get(entry.getKey()))){
                failedRecordMap.get(entry.getKey()).setVerifiedStatus(MD5Result.VerifiedStatus.INCONSISTENT);
            } else{
                failedRecordMap.get(entry.getKey()).setVerifiedStatus(MD5Result.VerifiedStatus.CONSISTENT);
            }
        }
    }


}
