package com.sap.ngom.datamigration.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class DBHashSqlGenerator {

    public String generatePostgresMd5Sql(String tableName, String tenant, JdbcTemplate jdbcTemplate){
      //  String retrivePrimaryKeySql = "select kc.column_name from information_schema.table_constraints tc join information_schema.key_column_usage kc on kc.table_name = \'" + tableName + "\' and kc.table_schema = \'public\' and kc.constraint_name = tc.constraint_name where tc.constraint_type = \'PRIMARY KEY\'  and kc.ordinal_position is not null";
       // String table_pk = jdbcTemplate.queryForObject(retrivePrimaryKeySql, String.class);


        String retrieveColumnInfoSql = "select column_name,data_type from information_schema.columns where table_schema=\'public\' AND (data_type=\'character varying\' OR data_type=\'integer\' OR data_type=\'bigint\') AND table_name="+ "\'" + tableName + "\' AND column_name !=\'tenant_id\'  order by column_name asc";

        Map<String,String> columnInfoMap = jdbcTemplate.query(retrieveColumnInfoSql, new ResultSetExtractor<Map<String,String>>() {
            @Override
            public Map<String,String> extractData(ResultSet resultSet) throws SQLException {
                Map map = new LinkedHashMap();
                while (resultSet.next()) {
                    map.put(resultSet.getString("column_name"), resultSet.getString("data_type"));

                }
                return map;
            }
        });

        StringBuilder md5SqlBuilder = new StringBuilder();
        String md5Sql = "";
        if(columnInfoMap.isEmpty()){
            return md5Sql;
        }

        for(String column:columnInfoMap.keySet()){
            switch (columnInfoMap.get(column)){
                case "character varying":
                    md5SqlBuilder.append("coalesce(").append(column).append(",\' \')||");
                    break;
                case "integer":
                    md5SqlBuilder.append("coalesce(").append(column).append(",0)||");
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length()-2,md5SqlBuilder.length());
        md5Sql = "select upper(md5(" + md5SqlBuilder.toString() + ")) from " + tableName + " where tenant_id=" + "\'" + tenant + "\'";
        return md5Sql;
    }


    public String generateHanaMd5Sql(String tableName, JdbcTemplate jdbcTemplate){
        String retrieveColumnInfoSql = "select COLUMN_NAME, DATA_TYPE_NAME from SYS.TABLE_COLUMNS WHERE TABLE_NAME=" +"\'" + tableName + "\'AND COLUMN_NAME !=\'TENANT_ID\' AND (DATA_TYPE_NAME=\'NVARCHAR\' OR DATA_TYPE_NAME=\'INTEGER\') ORDER BY COLUMN_NAME ASC";

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
        String md5Sql = "";
        StringBuilder md5SqlBuilder = new StringBuilder();
        if( columnInfoMap.isEmpty()){
            return md5Sql;
        }
        for(String column:columnInfoMap.keySet()){
            switch (columnInfoMap.get(column)){
                case "NVARCHAR":
                    md5SqlBuilder.append("to_varbinary(ifnull(").append(column).append(",\' \')),");
                    break;
                case "INTEGER":
                    md5SqlBuilder.append("to_varbinary(ifnull(").append(column).append(",0)),");
                    break;
            }
        }
        md5SqlBuilder.delete(md5SqlBuilder.length()-1,md5SqlBuilder.length());
        md5Sql = "select to_nvarchar(hash_md5(" + md5SqlBuilder.toString() + ")) from " + "\"" + tableName + "\"";
        return md5Sql;

    }
}
