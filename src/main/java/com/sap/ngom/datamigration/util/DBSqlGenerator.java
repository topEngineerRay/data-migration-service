package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.model.verification.TableInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Component
public class DBSqlGenerator {

    private static final String POSTGRES_COLUMN_NAME = "column_name";
    private static final String POSTGRES_COLUMN_TYPE = "udt_name";
    private static final String HANA_COLUMN_NAME = "COLUMN_NAME";
    private static final String HANA_COLUMN_TYPE = "DATA_TYPE_NAME";
    private static final String COMMA_DELIMITER = ",";
    
    public String generatePostgresMd5Sql(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {

        SortedMap<String, String> columnInfoMap = getAllColumnsNameAndTypeForPostgres(tableInfo, jdbcTemplate);
        StringBuilder md5SqlBuilder = new StringBuilder();
        final String operatorAND = "||";
        for (Map.Entry<String, String> entry : columnInfoMap.entrySet()) {
            switch (entry.getValue()) {
                case "varchar":
                case "text":
                case "bytea":
                    md5SqlBuilder.append("coalesce(").append(entry.getKey()).append(",\' \')").append(operatorAND);
                    break;
                case "int4":
                case "numeric":
                case "int8":
                case "int2":
                case "jsonb":
                case "uuid":
                case "bpchar":
                case "char":
                    md5SqlBuilder.append("coalesce(").append(entry.getKey()).append("::text,\' \')").append(operatorAND);
                    break;
                case "timestamp":
                    md5SqlBuilder.append("coalesce(to_char(").append(entry.getKey()).append(",\'YYYY-MM-DD HH24:MI:SS.MS\'),\' \')").append(operatorAND);
                    break;
                case "bool":
                    md5SqlBuilder.append("coalesce(").append(entry.getKey()).append("::integer::text,\' \')").append(operatorAND);
                    break;
                case "date":
                    md5SqlBuilder.append("coalesce(to_char(").append(entry.getKey()).append(",\'YYYY-MM-DD\'),\' \')").append(operatorAND);
                    break;
                default:
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length() - operatorAND.length(), md5SqlBuilder.length());
        return "select " + tableInfo.getPrimaryKey() + " as \"tablePrimaryKey\", upper(md5(" + md5SqlBuilder.toString() + ")) as \"md5Result\" from " + tableInfo.getSourceTableName() + " where " + tableInfo.getTenantColumnName() + "=\'" + tableInfo.getTenant() + "\'";
    }

    private SortedMap<String, String> getAllColumnsNameAndTypeForPostgres(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {
        String retrieveColumnInfoSql = "select column_name,udt_name from information_schema.columns where table_schema=\'public\' AND table_name=" + "\'" + tableInfo.getSourceTableName() + "\' AND column_name !=\'" + tableInfo.getTenantColumnName() + "\'";
        return jdbcTemplate.query(retrieveColumnInfoSql, new ResultSetExtractor<SortedMap<String, String>>() {
            @Override
            public SortedMap<String, String> extractData(ResultSet resultSet) throws SQLException {
                SortedMap<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                while (resultSet.next()) {
                    map.put(resultSet.getString(POSTGRES_COLUMN_NAME), resultSet.getString(POSTGRES_COLUMN_TYPE));
                }
                return map;
            }
        });
    }


    private SortedMap<String, String> getAllColumnsNameAndTypeForHANA(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {
        String retrieveColumnInfoSql = "select COLUMN_NAME, DATA_TYPE_NAME from SYS.TABLE_COLUMNS WHERE TABLE_NAME=" + "\'" + tableInfo.getTargetTableName() + "\' AND COLUMN_NAME !=\'" + tableInfo.getTenantColumnName().toUpperCase() + "\'";

        return jdbcTemplate.query(retrieveColumnInfoSql, new ResultSetExtractor<SortedMap<String, String>>() {
            @Override
            public SortedMap<String, String> extractData(ResultSet resultSet) throws SQLException {
                SortedMap<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                while (resultSet.next()) {
                    map.put(resultSet.getString(HANA_COLUMN_NAME), resultSet.getString(HANA_COLUMN_TYPE));

                }
                return map;
            }
        });
    }


    public String generateHanaMd5Sql(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {
        SortedMap<String, String> columnInfoMap = getAllColumnsNameAndTypeForHANA(tableInfo, jdbcTemplate);
        StringBuilder md5SqlBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : columnInfoMap.entrySet()) {
            switch (entry.getValue()) {
                case "NVARCHAR":
                case "BLOB":
                case "NCLOB":
                    md5SqlBuilder.append("to_varbinary(ifnull(").append(entry.getKey()).append(",\' \')),");
                    break;
                case "TIMESTAMP":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(entry.getKey()).append(",\'YYYY-MM-DD HH24:MI:SS.FF3\'),\' \')),");
                    break;
                case "TEXT":
                case "DECIMAL":
                case "INTEGER":
                case "TINYINT":
                case "SMALLINT":
                case "BIGINT":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(entry.getKey()).append("),\' \')),");
                    break;
                case "DATE":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(entry.getKey()).append(",\'YYYY-MM-DD\'),\' \')),");
                    break;
                default:
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length() - 1, md5SqlBuilder.length());
        return "select " + tableInfo.getPrimaryKey() + " as \"tablePrimaryKey\" , to_nvarchar(hash_md5(" + md5SqlBuilder.toString() + ")) as \"md5Result\"  from " + "\"" + tableInfo.getTargetTableName() + "\"";
    }


    public String generateSortedSelectAllSqlPostgres(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {
        SortedMap<String, String> columnInfoMap = getAllColumnsNameAndTypeForPostgres(tableInfo, jdbcTemplate);
        StringBuilder selectAllSqlBuilder = new StringBuilder("select ");
        selectAllSqlBuilder.append(tableInfo.getPrimaryKey()).append(" as \"tablePrimaryKey\", ");
        for (Map.Entry<String, String> entry : columnInfoMap.entrySet()) {
            switch(entry.getValue()){
                case "bool":
                    selectAllSqlBuilder.append(entry.getKey()).append("::integer").append(COMMA_DELIMITER);
                    break;
                case "timestamp":
                    selectAllSqlBuilder.append("to_char(").append(entry.getKey()).append(",\'YYYY-MM-DD HH24:MI:SS.MS\')").append(COMMA_DELIMITER);
                    break;
                default:
                    selectAllSqlBuilder.append(entry.getKey()).append(COMMA_DELIMITER);
                    break;
            }
        }
        selectAllSqlBuilder.delete(selectAllSqlBuilder.length() - COMMA_DELIMITER.length(), selectAllSqlBuilder.length());
        return selectAllSqlBuilder.append(" from ").append(tableInfo.getSourceTableName()).toString();
    }

    public String generateSortedSelectAllSqlHANA(TableInfo tableInfo, JdbcTemplate jdbcTemplate) {
        SortedMap<String, String> columnInfoMap = getAllColumnsNameAndTypeForHANA(tableInfo, jdbcTemplate);
        StringBuilder selectAllSqlHANABuilder = new StringBuilder("select ");
        selectAllSqlHANABuilder.append(tableInfo.getPrimaryKey()).append(" as \"tablePrimaryKey\", ");
        for (Map.Entry<String, String> entry : columnInfoMap.entrySet()) {
            if(entry.getValue().equals("TIMESTAMP")) {
                selectAllSqlHANABuilder.append("to_varchar(").append(entry.getKey()).append(",\'YYYY-MM-DD HH24:MI:SS.FF3\')").append(COMMA_DELIMITER);
            } else {
                selectAllSqlHANABuilder.append(entry.getKey()).append(COMMA_DELIMITER);
            }
        }
        selectAllSqlHANABuilder.delete(selectAllSqlHANABuilder.length() - COMMA_DELIMITER.length(), selectAllSqlHANABuilder.length());
        return selectAllSqlHANABuilder.append(" from ").append("\"").append(tableInfo.getTargetTableName()).append("\"").toString();

    }

    public List<String> getPrimaryKeysByTable(String tableName, JdbcTemplate jdbcTemplate) {
        String retrievePrimaryKeySql =
                "select kc.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kc on kc.table_name = \'"
                        + tableName
                        + "\' and kc.table_schema = \'public\' and kc.constraint_name = tc.constraint_name where tc.constraint_type = \'PRIMARY KEY\'  and kc.ordinal_position is not null order by column_name";
        return jdbcTemplate.queryForList(retrievePrimaryKeySql, String.class);
    }

    public String generateWhereStatFindSpecificPKSql(String primaryKey, Set<String> pkValues) {
        StringBuilder whereStatementFindSpecificPK = new StringBuilder(" where ");
        String[] pkColumnNames = primaryKey.split(COMMA_DELIMITER);

        for(String pkValue : pkValues) {
            String[] pkEachFieldValues = pkValue.split(COMMA_DELIMITER);
            whereStatementFindSpecificPK.append(" ( ");
            for(int i = 0; i < pkColumnNames.length; i++) {
                whereStatementFindSpecificPK.append(pkColumnNames[i]).append("= '").append(pkEachFieldValues[i]).append("' AND ");
            }
            whereStatementFindSpecificPK.delete(whereStatementFindSpecificPK.length() - 4,whereStatementFindSpecificPK.length());
            whereStatementFindSpecificPK.append(") OR ");
        }
        whereStatementFindSpecificPK.delete(whereStatementFindSpecificPK.length() - 3, whereStatementFindSpecificPK.length());
        return whereStatementFindSpecificPK.toString();
    }
}
