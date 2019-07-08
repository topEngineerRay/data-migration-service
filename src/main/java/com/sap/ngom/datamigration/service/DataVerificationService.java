package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.exception.DataVerificationException;
import com.sap.ngom.datamigration.model.*;
import com.sap.ngom.datamigration.model.verification.*;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.DBSqlGenerator;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private DBSqlGenerator dbSqlGenerator;

    private static final int MISMATCH_RECORDS_MAX_NUM = 100;
    private static final String MORE_INDICATOR = "..more";
    private static final String PK_DELIMITER = "||\',\'||";
    private static final String COMMA_DELIMITER = ",";
    private static final String PK_COLUMN_LABEL = "tablePrimaryKey";
    private static final String MD5_COLUMN_LABEL = "md5Result";
    private static final Integer THREADS_NUMBERS = 4;



    public ResponseMessage dataVerificationForOneTable(String tableName) {

        ResponseMessage responseMessage = new ResponseMessage();
        TableResult tableResult = verifyOneTable(tableName);

        if(tableResult.getDataConsistent().get()) {
            responseMessage.setStatus(Status.SUCCESS);
            responseMessage.setMessage("Data CONSISTENT between source and target after verification.");
            responseMessage.setDetail(null);
        } else{
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

        List<String> tableList = dbConfigReader.getSourceTableNames();
        ResponseMessage responseMessage = new ResponseMessage();
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        List<TableResult> tablesResultList = new ArrayList<>();
        CompletionService<TableResult> completionService = new ExecutorCompletionService<>(executorService);
        List<Future<TableResult>> futureResultList = new ArrayList<>();

        for(String tableName : tableList) {
            futureResultList.add(completionService.submit( () -> verifyOneTable(tableName)));
        }

        try {
            while (!futureResultList.isEmpty()) {
                Future<TableResult> future = completionService.take();
                futureResultList.remove(future);
                TableResult tableResult = future.get();
                if (!tableResult.getDataConsistent().get()) {
                    tablesResultList.add(tableResult);
                }
            }
            awaitTerminationAfterShutdown(executorService);
        }catch (ExecutionException e) {
            executorService.shutdownNow();
            throw new DataVerificationException("Data verification failed due to ExecutionException when verifying all tables, details: " + e.getMessage());
        } catch (InterruptedException e) {
            log.error("Data Verification failed due to InterruptedException when verifying all tables, details:{} ",e.getMessage());
            Thread.currentThread().interrupt();
        }


        if(tablesResultList.isEmpty()){
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


    private TableResult verifyOneTable(String tableName) {

        log.info("Data verification starting for table [{}].", tableName);
        TableResult tableResult = new TableResult();
        tableResult.setDataConsistent(new AtomicBoolean(true));
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        TableInfo tableInfo = initialTableInfo(tableName, jdbcTemplate);
        List<TenantResult> tenantsResultList = new ArrayList<>();
        List<String> tenants = tenantHelper.getAllTenants(tableName);
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CompletionService<TenantResult> completionService = new ExecutorCompletionService<>(executorService);
        List<Future<TenantResult>> futureResultList = new ArrayList<>();
        for(String tenant: tenants) {
            futureResultList.add(completionService.submit(() -> verifyOneTenant(tableInfo,tenant)));
        }
        try{
            while(!futureResultList.isEmpty()) {
                Future<TenantResult> future = completionService.take();
                futureResultList.remove(future);
                TenantResult tenantResult = future.get();
                if (tenantResult.getTenant() != null) {
                    tableResult.getDataConsistent().set(false);
                    tenantsResultList.add(tenantResult);
                }
            }
            awaitTerminationAfterShutdown(executorService);
        } catch (ExecutionException e) {
            executorService.shutdownNow();
            throw new DataVerificationException("Data verification failed due to ExecutionException when verifying table [ " + tableName + "], details: " + e.getMessage());
        } catch (InterruptedException e) {
            log.error("Data Verification failed due to InterruptedException when when verifying table[{}], details:{} ",tableName,e.getMessage());
            Thread.currentThread().interrupt();
        }

        if(!tableResult.getDataConsistent().get()){
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
            if(!tableInfo.getOriginalPrimaryKey().isEmpty()){
                tableResult.setPrimaryKey(tableInfo.getOriginalPrimaryKey());
            }
        }
        log.info("Data verification done for table [{}].",tableName);
        return tableResult;
    }

    private TenantResult verifyOneTenant(TableInfo tableInfo, String tenant) {
        log.info("Data verification starting for table [{}] -> tenant [{}]", tableInfo.getSourceTableName(),tenant);
        TenantThreadLocalHolder.setTenant(tenant);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
        final int postgresRecordsCount = jdbcTemplate.queryForObject("select count(*) from " + tableInfo.getSourceTableName() + " where " + tableInfo.getTenantColumnName() + " = \'" + tableInfo.getTenant() + "\'", Integer.class);
        final int hanaRecordsCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + tableInfo.getTargetTableName() + "\"", Integer.class);
        CountResult countResult = new CountResult();
        TenantResult tenantResult = new TenantResult();
        boolean tenantDataConsistent = true;
        if (postgresRecordsCount != hanaRecordsCount) {
            tenantDataConsistent = false;
            log.warn("Data record count does not match between source and target for table [{}] -> tenant [{}]",tableInfo.getSourceTableName(),tenant);
        } else if (hanaRecordsCount != 0 && !tableInfo.getPrimaryKey().isEmpty()) {
            //MD5 content check
            List<String> failedRecords = md5Check(tableInfo).entrySet().stream()
                                        .map(Map.Entry::getKey).collect(Collectors.toList());

            if (!failedRecords.isEmpty()) {
                tenantDataConsistent = false;
                tenantResult.setInconsistentRecords(failedRecords);
                log.warn("Data content integrity check has detected {} mismatching records between source and target for table [{}] -> tenant [{}]",failedRecords.size(),tableInfo.getSourceTableName(),tenant);
            }
        }
        if (!tenantDataConsistent) {
            countResult.setSourceCount(postgresRecordsCount);
            countResult.setTargetCount(hanaRecordsCount);
            tenantResult.setTenant(tenant);
            tenantResult.setCountResult(countResult);
        }

        log.info("Data verification done for table [{}] -> tenant [{}]", tableInfo.getSourceTableName(),tenant);
        return tenantResult;
    }


    public void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(300, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Data Verification failed due to InterruptedException when shutdown ExecutorService pool, details:{}.",e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private Map<String, FailedRecordStatus> md5Check(TableInfo tableInfo) {
        JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
        JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);

        final String hanaMd5Sql = dbSqlGenerator.generateHanaMd5Sql(tableInfo,hanaJdbcTemplate);
        Map<String,String> hanaMd5Result = getPKAndMD5ValueFromDB(hanaJdbcTemplate,hanaMd5Sql);
        String postgresMd5Sql = tableInfo.getPostgresMd5Sql() + " where " + tableInfo.getTenantColumnName() + "=\'" + tableInfo.getTenant() + "\'";
        Map<String,String> postgresMd5Result = getPKAndMD5ValueFromDB(postgresJdbcTemplate,postgresMd5Sql);
        Map<String, FailedRecordStatus> failedRecordMap = new LinkedHashMap<>();
        for(Map.Entry<String, String> entry : postgresMd5Result.entrySet()){
            final String recordPkValue = entry.getKey();
            final String recordMd5ValuePostgres = entry.getValue();

            if(!hanaMd5Result.containsKey(recordPkValue)) {
                failedRecordMap.put(recordPkValue, FailedRecordStatus.INCONSISTENT);
            } else if(!hanaMd5Result.get(recordPkValue).equals(recordMd5ValuePostgres)) {
                failedRecordMap.put(recordPkValue, FailedRecordStatus.CHECKAGAIN);
            }
            if(failedRecordMap.size() >= MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                updateFailedRecordsMapByJavaEquals(failedRecordMap, tableInfo);
            }
            if(failedRecordMap.size() >= MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                log.warn("Data content integrity check only inspected the first {} records for table [{}] -> tenant [{}] , since the inconsistent records reach the predefined max number.", MISMATCH_RECORDS_MAX_NUM, tableInfo.getSourceTableName(),tableInfo.getTenant());
                break;
            }
        }

        //Java Equals check for current emoji issue (same emoji generate different md5 value in two dbs)
        updateFailedRecordsMapByJavaEquals(failedRecordMap, tableInfo);
        if(failedRecordMap.size() >= MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ) {
            failedRecordMap.put(MORE_INDICATOR,FailedRecordStatus.INCONSISTENT);
        }
        return failedRecordMap;
    }

    private void updateFailedRecordsMapByJavaEquals(Map<String, FailedRecordStatus> failedRecordMap, TableInfo tableInfo) {
        Map<String, FailedRecordStatus> needDoubleCheckRecordsMap = filterMapOnVerifiedStatus(failedRecordMap, FailedRecordStatus.CHECKAGAIN);

        if(needDoubleCheckRecordsMap.isEmpty()){
            return;
        }
        log.info("Java Object equals check starting for {} records in table [{}] -> tenant [{}]",needDoubleCheckRecordsMap.size(),tableInfo.getSourceTableName(),tableInfo.getTenant());

        final JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
        final JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);
        final String whereStatement = dbSqlGenerator.generateWhereStatFindSpecificPKSql(tableInfo.getPrimaryKey().replace(PK_DELIMITER, COMMA_DELIMITER),needDoubleCheckRecordsMap.keySet());
        final String selectAllSqlPostgres = dbSqlGenerator.generateSortedSelectAllSqlPostgres(tableInfo, postgresJdbcTemplate) + whereStatement + " AND " + tableInfo.getTenantColumnName() + "=\'" + tableInfo.getTenant() + "\'";
        final String selectAllSqlHANA = dbSqlGenerator.generateSortedSelectAllSqlHANA(tableInfo,hanaJdbcTemplate) + whereStatement;

        Map<String,List<String>> getRelevantRecordsPostgres = getPKAndWholeFieldsFromDB(postgresJdbcTemplate, selectAllSqlPostgres);
        Map<String,List<String>> getRelevantRecordsHANA = getPKAndWholeFieldsFromDB(hanaJdbcTemplate, selectAllSqlHANA);

        for(Map.Entry<String, List<String>> entry : getRelevantRecordsPostgres.entrySet()) {
            final String pkValue = entry.getKey();
            if(!entry.getValue().equals(getRelevantRecordsHANA.get(pkValue))){
                failedRecordMap.put(pkValue, FailedRecordStatus.INCONSISTENT);
            } else{
                failedRecordMap.remove(pkValue);
            }
        }
    }
    private Map<String, String> getPKAndMD5ValueFromDB(JdbcTemplate jdbcTemplate, String sql) {
        return jdbcTemplate.query(sql, new ResultSetExtractor<Map<String,String>>() {
            @Override
            public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                Map<String, String> map = new HashMap<>();
                while (resultSet.next()) {
                    map.put(resultSet.getString(PK_COLUMN_LABEL), resultSet.getString(MD5_COLUMN_LABEL));
                }
                return map;
            }
        });
    }

    private Map<String, List<String>> getPKAndWholeFieldsFromDB(JdbcTemplate jdbcTemplate, String sql) {
        return jdbcTemplate.query(sql, new ResultSetExtractor<Map<String,List<String>>>() {
            @Override
            public Map<String,List<String>> extractData(ResultSet resultSet) throws SQLException {
                final int columnCount = resultSet.getMetaData().getColumnCount();
                final Map<String,List<String>> recordList = new HashMap<>();

                while(resultSet.next()){
                    final List<String> fieldValues = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        if (resultSet.getString(i) != null) {
                            fieldValues.add(resultSet.getString(i));
                        }
                    }
                    recordList.put(resultSet.getString(PK_COLUMN_LABEL),fieldValues);
                }
                return recordList;
            }
        });
    }

    private TableInfo initialTableInfo(String tableName, JdbcTemplate jdbcTemplate) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setSourceTableName(tableName);
        tableInfo.setTargetTableName(dbConfigReader.getTargetTableName(tableName));
        tableInfo.setTenantColumnName(tenantHelper.determineTenant(tableName));

        List<String> originalTablePrimaryKeyList = dbSqlGenerator.getPrimaryKeysByTable(tableName, jdbcTemplate);
        List<String> tablePrimaryKeyList = new ArrayList<>(originalTablePrimaryKeyList);
        tableInfo.setOriginalPrimaryKey(concatPKWithDelimiter(originalTablePrimaryKeyList,PK_DELIMITER));

        // Special handling: remove tenant_id column when it is part of composite primary key.
        tablePrimaryKeyList.remove(tableInfo.getTenantColumnName());
        tableInfo.setPrimaryKey(concatPKWithDelimiter(tablePrimaryKeyList,PK_DELIMITER));
        if(tablePrimaryKeyList.isEmpty()) {
            log.warn("Data content integrity check would be skipped for table [{}], since it doesn't contain primary key.", tableName);
        }
        //It needs after set primary key.
        String postgresMd5Sql = dbSqlGenerator.generatePostgresMd5Sql(tableInfo, jdbcTemplate);
        tableInfo.setPostgresMd5Sql(postgresMd5Sql);

        return tableInfo;
    }

    private String concatPKWithDelimiter(List<String> tablePrimaryKeyList, final String Delimiter) {
        if(tablePrimaryKeyList.isEmpty()){
            return "";
        }
        StringBuilder tablePrimaryKeyBuilder = new StringBuilder();
        for(String primaryKeyField:tablePrimaryKeyList){
            tablePrimaryKeyBuilder.append(primaryKeyField).append(Delimiter);
        }
        return tablePrimaryKeyBuilder.delete(tablePrimaryKeyBuilder.length()-Delimiter.length(), tablePrimaryKeyBuilder.length()).toString();
    }

    private Map<String, FailedRecordStatus> filterMapOnVerifiedStatus(Map<String, FailedRecordStatus> recordsMap, FailedRecordStatus verifiedStatus) {
        return recordsMap.entrySet().stream()
                .filter( x -> x.getValue().equals(verifiedStatus))
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
    }
}
