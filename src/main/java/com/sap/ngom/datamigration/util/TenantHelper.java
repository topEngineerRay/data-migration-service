package com.sap.ngom.datamigration.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;

@Component
public class TenantHelper {
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;


    public List<String> getAllTenants(String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        String sql = "select distinct tenant_id from " + tableName;
        List<String> allTenantsList = jdbcTemplate.queryForList(sql, String.class);
        return allTenantsList;
    }

    @Bean
    public JdbcTemplate sourcJdbcTemplate(){
        return new JdbcTemplate(sourceDataSource);
    }
}
