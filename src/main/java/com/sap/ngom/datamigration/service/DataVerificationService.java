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
    private static final String COMMA_DELIMITER = ",";
    private static final String PK_COLUMN_LABEL = "tablePrimaryKey";
    private static final String MD5_COLUMN_LABEL = "md5Result";



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
        TableInfo tableInfo = initialTableInfo(tableName, jdbcTemplate);
        String sqlForTenantAndCount = "select count(" + tableInfo.getTenantColumnName() + ") as tenant_count, " + tableInfo.getTenantColumnName() + " from " + tableName + " where " + tableInfo.getTenantColumnName() + " is not null group by " + tableInfo.getTenantColumnName();

        Map<String,Integer> tenantAndCountMap = jdbcTemplate.query(sqlForTenantAndCount, resultSet -> {
            Map<String, Integer> map = new HashMap<>();
            while (resultSet.next()) {
                map.put(resultSet.getString(tableInfo.getTenantColumnName()), resultSet.getInt("tenant_count"));

            }
            return map;
        });

        List<TenantResult> tenantsResultList = new ArrayList<>();
        tableResult.setDataConsistent(true);
        for (Map.Entry<String,Integer> entry : tenantAndCountMap.entrySet()) {
            final String tenant = entry.getKey();
            final int postgresRecordsCount = entry.getValue();
            TenantThreadLocalHolder.setTenant(tenant);
            tableInfo.setTenant(tenant);
            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
            int hanaRecordsCount = hanaJdbcTemplate.queryForObject("select count(*) from " + "\"" + tableInfo.getTargetTableName() + "\"",Integer.class);
            CountResult countResult = new CountResult();
            TenantResult tenantResult = new TenantResult();
            boolean tenantDataConsistent = true;
            if(postgresRecordsCount != hanaRecordsCount){
                tableResult.setDataConsistent(false);
                tenantDataConsistent = false;
            } else if( hanaRecordsCount != 0 && !tableInfo.getPrimaryKey().isEmpty()){

                //MD5 content check
                Map<String, VerifiedStatus> failedRecordMap = md5Check(tableInfo);

                //Java Equals check for current emoji issue (same emoji generate different md5 value in two dbs)
                checkRecordsIfActualEqual(filterMapOnVerifiedStatus(failedRecordMap,VerifiedStatus.CHECKAGAIN), tableInfo);

                List<String> failedRecords = failedRecordMap.entrySet().stream()
                                .filter(x -> x.getValue().equals(VerifiedStatus.INCONSISTENT))
                                .map( x -> x.getKey()).collect(Collectors.toList());

                if(!failedRecords.isEmpty()) {
                    tableResult.setDataConsistent(false);
                    tenantDataConsistent = false;
                    tenantResult.setInconsistentRecords(failedRecords);
                }
            }

            if(!tenantDataConsistent){
                countResult.setSourceCount(tenantAndCountMap.get(tenant));
                countResult.setTargetCount(hanaRecordsCount);
                tenantResult.setTenant(tenant);
                tenantResult.setCountResult(countResult);
                tenantsResultList.add(tenantResult);
            }
            log.info("Data verification is completed for tenant (" + tenant + ") in table: " + tableName);
        }
        if(!tableResult.getDataConsistent()){
            tableResult.setTenants(tenantsResultList);
            tableResult.setTable(tableName);
            if(!tableInfo.getPrimaryKey().isEmpty()){
                tableResult.setPrimaryKey(tableInfo.getPrimaryKey().replace(PK_DELIMITER,COMMA_DELIMITER));
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

    private Map<String, VerifiedStatus> filterMapOnVerifiedStatus(Map<String, VerifiedStatus> recordsMap, VerifiedStatus verifiedStatus) {
        return recordsMap.entrySet().stream()
                .filter( x -> x.getValue().equals(verifiedStatus))
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
    }


    private Map<String, VerifiedStatus> md5Check(TableInfo tableInfo) {
        log.info("Hash consistent check is starting for tenant (" + tableInfo.getTenant() + ") in table: " + tableInfo.getSourceTableName());
        JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
        JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);

        String hanaMd5Sql = dbHashSqlGenerator.generateHanaMd5Sql(tableInfo,hanaJdbcTemplate);
        Map<String,String> hanaMd5Result = getPKAndMD5ValueFromDB(hanaJdbcTemplate,hanaMd5Sql);
        String postgresMd5Sql = dbHashSqlGenerator.generatePostgresMd5Sql(tableInfo, postgresJdbcTemplate);
        Map<String,String> postgresMd5Result = getPKAndMD5ValueFromDB(postgresJdbcTemplate,postgresMd5Sql);

        Map<String, VerifiedStatus> failedRecordMap = new HashMap<>();
        for(Map.Entry<String, String> entry : postgresMd5Result.entrySet()){
            final String recordPkValue = entry.getKey();
            final String recordMd5ValuePostgres = entry.getValue();

            if(!hanaMd5Result.containsKey(recordPkValue)) {
                failedRecordMap.put(recordPkValue, VerifiedStatus.INCONSISTENT);
            } else if(!hanaMd5Result.get(recordPkValue).equals(recordMd5ValuePostgres)) {
                failedRecordMap.put(recordPkValue, VerifiedStatus.CHECKAGAIN);
            }
            if(failedRecordMap.size() == MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                Map<String, VerifiedStatus> needDoubleCheckFailedRecords = failedRecordMap.entrySet().stream()
                        .filter( x -> x.getValue().equals(VerifiedStatus.CHECKAGAIN))
                        .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                checkRecordsIfActualEqual(needDoubleCheckFailedRecords, tableInfo);
            }
            if(failedRecordMap.size() == MISMATCH_RECORDS_MAX_NUM && postgresMd5Result.size() > MISMATCH_RECORDS_MAX_NUM ){
                failedRecordMap.put(MORE_INDICATOR, VerifiedStatus.INCONSISTENT);
                log.warn("Data verification only checked the first " + MISMATCH_RECORDS_MAX_NUM + " records for tenant (" + tableInfo.getTenant() + ") in table: " + tableInfo.getSourceTableName() +", as the inconsistent records reaches the predefined max number.");
                break;
            }
        }

        return failedRecordMap;
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
        return tableInfo;
    }

    private void checkRecordsIfActualEqual(Map<String, VerifiedStatus> failedRecordMap, TableInfo tableInfo) {
        if(failedRecordMap.isEmpty()){
            return;
        }

        final JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
        final JdbcTemplate postgresJdbcTemplate = new JdbcTemplate(sourceDataSource);
        final String whereStatement = dbHashSqlGenerator.generateWhereStatFindSpecificPKSql(tableInfo.getPrimaryKey().replace(PK_DELIMITER, COMMA_DELIMITER),failedRecordMap.keySet());
        final String selectAllSqlPostgres = dbHashSqlGenerator.generateSortedSelectAllSqlPostgres(tableInfo, postgresJdbcTemplate) + whereStatement ;
        final String selectAllSqlHANA = dbHashSqlGenerator.generateSortedSelectAllSqlHANA(tableInfo,hanaJdbcTemplate) + whereStatement;

        Map<String,List<String>> getRelevantRecordsPostgres = getPKAndWholeFieldsFromDB(postgresJdbcTemplate, selectAllSqlPostgres);

        Map<String,List<String>> getRelevantRecordsHANA = getPKAndWholeFieldsFromDB(hanaJdbcTemplate, selectAllSqlHANA);

        for(Map.Entry<String, List<String>> entry : getRelevantRecordsPostgres.entrySet()) {
            final String pkValue = entry.getKey();
            if(!entry.getValue().equals(getRelevantRecordsHANA.get(pkValue))){
                failedRecordMap.put(pkValue,VerifiedStatus.INCONSISTENT);
            } else{
                failedRecordMap.put(pkValue,VerifiedStatus.CONSISTENT);
            }
        }
    }
}
