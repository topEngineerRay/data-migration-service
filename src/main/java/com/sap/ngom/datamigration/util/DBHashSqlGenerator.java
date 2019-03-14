package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.model.verification.TableInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class DBHashSqlGenerator {

    public String generatePostgresMd5Sql(TableInfo tableInfo, JdbcTemplate jdbcTemplate){

        String retrieveColumnInfoSql = "select column_name,udt_name from information_schema.columns where table_schema=\'public\' AND table_name="+ "\'" + tableInfo.getSourceTableName() + "\' AND column_name !=\'" + tableInfo.getTenantColumnName() + "\' order by column_name asc";

        Map<String,String> columnInfoMap = jdbcTemplate.query(retrieveColumnInfoSql, new ResultSetExtractor<Map<String,String>>() {
            @Override
            public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                Map map = new LinkedHashMap();
                while (resultSet.next()) {
                    map.put(resultSet.getString("column_name"), resultSet.getString("udt_name"));

                }
                return map;
            }
        });

        StringBuilder md5SqlBuilder = new StringBuilder();

        for(String column:columnInfoMap.keySet()){
            switch (columnInfoMap.get(column)){
                case "varchar":
                case "char":
                case "bpchar":
                case "text":
                case "bytea":
                    md5SqlBuilder.append("coalesce(").append(column).append(",\' \')||");
                    break;
                case "int8":
                case "int4":
                case "int2":
                    md5SqlBuilder.append("coalesce(").append(column).append(",0)||");
                    break;
                case "timestamp":
                    md5SqlBuilder.append("coalesce(to_char(").append(column).append(",\'YYYY/MM/DD HH:mm:ss\'),\' \')||");
                    break;
                case "bool":
                    md5SqlBuilder.append("coalesce(").append(column).append("::integer::text,\' \')||");
                    break;
                case "date":
                    md5SqlBuilder.append("coalesce(to_char(").append(column).append(",\'YYYY/MM/DD\'),\' \')||");
                    break;
                case "numeric":
                case "jsonb":
                case "uuid":
                    md5SqlBuilder.append("coalesce(").append(column).append("::text,\' \')||");
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length()-2,md5SqlBuilder.length());
        return "select row(" + tableInfo.getPrimaryKey() +") as \"tablePrimaryKey\", upper(md5(" + md5SqlBuilder.toString() + ")) as \"md5Result\" from " + tableInfo.getSourceTableName() + " where " + tableInfo.getTenantColumnName() + "=\'" + tableInfo.getTenant() + "\' order by " + tableInfo.getPrimaryKey();
    }


    public String generateHanaMd5Sql(TableInfo tableInfo, JdbcTemplate jdbcTemplate){
        String retrieveColumnInfoSql = "select COLUMN_NAME, DATA_TYPE_NAME from SYS.TABLE_COLUMNS WHERE TABLE_NAME=" +"\'" + tableInfo.getTargetTableName() + "\' AND COLUMN_NAME !=\'" + tableInfo.getTenantColumnName().toUpperCase() + "\' ORDER BY COLUMN_NAME ASC";

        Map<String,String> columnInfoMap = jdbcTemplate.query(retrieveColumnInfoSql, new ResultSetExtractor<Map<String,String>>() {
            @Override
            public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                Map map = new LinkedHashMap();
                while (resultSet.next()) {
                    map.put(resultSet.getString("COLUMN_NAME"), resultSet.getString("DATA_TYPE_NAME"));

                }
                return map;
            }
        });
        StringBuilder md5SqlBuilder = new StringBuilder();

        for(String column:columnInfoMap.keySet()){
            switch (columnInfoMap.get(column)){
                case "NVARCHAR":
                case "BLOB":
                case "NCLOB":
                    md5SqlBuilder.append("to_varbinary(ifnull(").append(column).append(",\' \')),");
                    break;
                case "INTEGER":
                    md5SqlBuilder.append("to_varbinary(ifnull(").append(column).append(",0)),");
                    break;
                case "TIMESTAMP":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(column).append(",\'YYYY/MM/DD HH:mm:ss\'),\' \')),");
                    break;
                case "TEXT":
                case "DECIMAL":
                case "TINYINT":
                case "SMALLINT":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(column).append("),\' \')),");
                    break;
                case "DATE":
                    md5SqlBuilder.append("to_varbinary(ifnull(to_varchar(").append(column).append(",\'YYYY/MM/DD\'),\' \')),");
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length()-1,md5SqlBuilder.length());
        return "select to_nvarchar(hash_md5(" + md5SqlBuilder.toString() + ")) from " + "\"" + tableInfo.getTargetTableName() + "\" order by " + tableInfo.getPrimaryKey();
    }
}
