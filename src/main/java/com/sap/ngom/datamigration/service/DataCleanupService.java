package com.sap.ngom.datamigration.service;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.TenantSpecificHANAMultitRoutingDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service @Log4j2
public class DataCleanupService {
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("MTRoutingDataSource")
    private DataSource destinationDataSource;

    private String targetNamespace = "com.sap.ngom.db::BusinessPartner.";
    private static final Integer THREADS_NUMBERS = 5;

    public void cleanData4OneTable(String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        List<String> tenantList = jdbcTemplate.query("select tenant_id from " + tableName + " group by tenant_id", new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet resultSet, int i) throws SQLException {
                System.out.println("i: " + i);
                return resultSet.getString("tenant_id");
            }
        });

        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tenantLatch = new CountDownLatch(tenantList.size());

        for (String tenant : tenantList) {
            executorService.submit(() -> {
                //change data source
                TenantSpecificHANAMultitRoutingDataSource.setTenant(tenant);

                JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(destinationDataSource);
                hanaJdbcTemplate.execute("delete from " + "\"" + targetNamespace + tableName + "\"");

                tenantLatch.countDown();
                log.info("Cleanup done for tenant: " + tenant + " in table: " + tableName);
            });
        }

        try {
            tenantLatch.await(); // wait until latch counted down to 0
        } catch (InterruptedException e) {
            log.error("Unexpected error occurs:", e);
        }
        executorService.shutdown();
        log.info("Cleanup done for all tenants in table: " + tableName);
    }

    public void cleanData4AllTables() {
        List<String> tableList = new ArrayList<>();
        tableList.add("bp");
        tableList.add("address");
        tableList.add("market");
        tableList.add("bprelationship");
        tableList.add("customreference");
        tableList.add("externalinfo");
        tableList.add("objectreplicationstatus");

        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tableLatch = new CountDownLatch(tableList.size());

        for (String tableName : tableList) {
            executorService.submit(() -> {
                cleanData4OneTable(tableName);
                tableLatch.countDown();
                log.info("Cleanup done for table: " + tableName);
            });
        }
        try {
            tableLatch.await(); // wait until latch counted down to 0
        } catch (InterruptedException e) {
            log.error("Unexpected error occurs:", e);
        }
        executorService.shutdown();
        log.info("Cleanup done for all tables." );
    }
}
