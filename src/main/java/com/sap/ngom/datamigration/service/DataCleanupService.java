package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.TenantSpecificHANAMultitRoutingDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class DataCleanupService {
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("MTRoutingDataSource")
    DataSource destinationDataSource;

    public void cleanData4OneTable(String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        List<String> tenantList = jdbcTemplate.query("select tenant_id from " + tableName + " group by tenant_id", new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet resultSet, int i) throws SQLException {
                System.out.println("i: " + i);
                return resultSet.getString("tenant_id");
            }
        });

        tenantList.forEach(tenant -> {
            //change data source
            TenantSpecificHANAMultitRoutingDataSource.setTenant(tenant);

            JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(destinationDataSource);
            hanaJdbcTemplate.execute("delete from " + tableName + "");

            System.out.println("Cleanup done for tenant: " + tenant);

        });

        System.out.println("Cleanup done for all tenants.");

    }
}
