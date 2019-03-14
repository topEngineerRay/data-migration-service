package com.sap.ngom.datamigration.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class TenantHelper {
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    public List<String> getAllTenants(String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        String tenantName = determineTenant(tableName);
        String sql = "select distinct " + tenantName + " from " + tableName + " where " + tenantName + " is not null";
        List<String> allTenantsList = jdbcTemplate.queryForList(sql, String.class);
        return allTenantsList;
    }

    public String determineTenant(String tableName) {
        String defalutTenantName = "tenant_id";
        String tenantName = "tenant";
        //the table list whose tenantid named tenant
        List<String> tablelistForTenant = Arrays
                .asList("custom_activity_status_item", "custom_activity_status_order", "flow_step_status_v2",
                        "flow_per_object",
                        "t_trigger_events_processed", "t_order_released", "technical_resource_status");
        if (tablelistForTenant.contains(tableName)) {
            return tenantName;
        }
        return defalutTenantName;
    }
}
