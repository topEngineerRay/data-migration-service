package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.configuration.hana.TenantThreadLocalHolder;
import com.sap.ngom.datamigration.exception.DataCleanupException;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.TenantHelper;
import com.sap.ngom.util.hana.db.exceptions.HanaDataSourceDeterminationException;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service @Log4j2
public class DataCleanupService {
    @Autowired
    @Qualifier("targetDataSource")
    private DataSource targetDataSource;

    @Autowired
    TenantHelper tenantHelper;

    @Autowired
    DBConfigReader dbConfigReader;

    private static final Integer THREADS_NUMBERS = 10;

    public void cleanData4OneTable(String tableName) {
        String targetTableName = dbConfigReader.getTargetTableName(tableName);

        List<String> tenantList = tenantHelper.getAllTenants(tableName);
        List<String> tableList = new ArrayList<String>();
        tableList.add(targetTableName);
        ExecuteCleanup(tenantList, tableList);
    }

    public void cleanData4AllTables() {
        List<String> tableList = dbConfigReader.getSourceTableNames();
        List<String> tenantList = new ArrayList<String>();
        Set<String> tenantSet = new HashSet<>();
        List<String> targetTableList = new ArrayList<String>();

        for (String tableName : tableList) {
            String targetTableName = dbConfigReader.getTargetTableName(tableName);
            targetTableList.add(targetTableName);

            tenantList = tenantHelper.getAllTenants(tableName);
            tenantSet.addAll(tenantList);
        }
        List allTenants = new ArrayList(tenantSet);
        ExecuteCleanup(allTenants, targetTableList);

        log.info("[Cleanup][all] ******* Cleanup done for all tables." );
    }

    private void ExecuteCleanup(List<String> tenantList, List<String> tableList)  {
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tenantLatch = new CountDownLatch(tenantList.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);

        for (String tenant : tenantList) {
            executorService.submit(() -> {
                //change data source
                TenantThreadLocalHolder.setTenant(tenant);
                JdbcTemplate hanaJdbcTemplate = new JdbcTemplate(targetDataSource);
                for (String tableName : tableList) {
                    try {
                        log.info("Execute SQL for tenant " + tenant + ": TRUNCATE TABLE " + tableName + '.');
                        hanaJdbcTemplate.execute("TRUNCATE TABLE " + "\"" + tableName + "\"");
                    } catch (HanaDataSourceDeterminationException | DataAccessException e) {
                        hasError.set(true);
                        log.error("[Cleanup] Exception occurs when execute SQL DELETE for tenant: " + tenant + " in table: " + tableName + ": ", e);
                    }
                }

                tenantLatch.countDown();
                log.info("[Cleanup] Cleanup done for tenant: " + tenant + ". " + tenantLatch.getCount() + "/" + tenantList.size());
            });
        }

        try {
            if(!tenantLatch.await(900, TimeUnit.SECONDS)) {
                log.error("[Cleanup] Timeout for cleanup [900 seconds]");
            }
        } catch (InterruptedException e) {
            log.error("[Cleanup] tenantLatch.await interrupted exception occurs:", e);
        }
        executorService.shutdownNow();

        if(hasError.get() || tenantLatch.getCount() != 0) {
            throw new DataCleanupException("[Cleanup] Error occurs when delete data. Search logs with keyword '[Cleanup] Exception' or '[Cleanup] Timeout'");
        }
        log.info("[Cleanup] <<<<< Cleanup done. ");
    }
}
