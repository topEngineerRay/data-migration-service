package com.sap.ngom.datamigration.util;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;

@Component
public class DataMigrationServiceUtil {
    public List<String> getAllTenants(String tableName, DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "select distinct tenant_id from " + tableName;
        List<String> allTenantsList = jdbcTemplate.queryForList(sql, String.class);
        return allTenantsList;
    }
}
