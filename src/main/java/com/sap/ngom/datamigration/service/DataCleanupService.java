package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.exception.DataCleanupException;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.DataMigrationServiceUtil;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service @Log4j2
public class DataCleanupService {
    @Autowired
    @Qualifier("sourceDataSource")
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("targetDataSource")
    private DataSource targetDataSource;

    @Autowired
    DataMigrationServiceUtil dataMigrationServiceUtil;

    @Autowired
    DBConfigReader dbConfigReader;

    private static final Integer THREADS_NUMBERS = 5;

    public void cleanData4OneTable(String tableName) throws Exception {
        String targetTableName = dbConfigReader.getTargetTableName(tableName);

        List<String> tenantList = dataMigrationServiceUtil.getAllTenants(tableName, sourceDataSource);
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tenantLatch = new CountDownLatch(tenantList.size());

        for (String tenant : tenantList) {
            executorService.submit(() -> {
                //change data source
                TenantThreadLocalHolder.setTenant(tenant);

                JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
                hanaJdbcTemplate.execute("delete from " + "\"" + targetTableName + "\"");

                tenantLatch.countDown();
                log.info("Cleanup done for tenant: " + tenant + " in table: " + tableName);
            });
        }

        try {
            if(!tenantLatch.await(180, TimeUnit.SECONDS)) {
                throw new DataCleanupException("Timeout for table: " + tableName);
            }
        } catch (InterruptedException e) {
            log.error("Unexpected error occurs:", e);
        }
        executorService.shutdown();
        log.info("Cleanup done for all tenants in table: " + tableName);
    }

    public void cleanData4AllTables() throws Exception{
        List<String> tableList = dbConfigReader.getSourceTableNames();

        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tableLatch = new CountDownLatch(tableList.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);

        for (String tableName : tableList) {
            executorService.submit(() -> {
                try {
                    cleanData4OneTable(tableName);
                } catch (Exception e) {
                    hasError.set(true);
                }
                tableLatch.countDown();
                log.info("Cleanup done for table: " + tableName);
            });
        }
        try {
            if(!tableLatch.await(180, TimeUnit.SECONDS)) {
                log.error("Timeout for all tables");
            }
        } catch (InterruptedException e) {
            log.error("Unexpected error occurs:", e);
        }
        if(hasError.get() || tableLatch.getCount() != 0) {
            throw new DataCleanupException("Error occurs when delete data in tables");
        }
        executorService.shutdown();
        log.info("Cleanup done for all tables." );
    }
}
