package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.InstanceManagerUtil;
import com.sap.ngom.datamigration.util.TenantHelper;
import com.sap.ngom.util.hana.db.configuration.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.exceptions.HDIDeploymentException;
import com.sap.ngom.util.hana.db.exceptions.HDIDeploymentInitializerException;
import com.sap.ngom.util.hana.db.exceptions.HanaDataSourceDeterminationException;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;
import com.sap.xsa.core.instancemanager.client.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Log4j2
public class InitializerService {
    @Autowired
    @Qualifier("targetDataSource")
    private DataSource targetDataSource;

    @Autowired
    TenantHelper tenantHelper;

    @Autowired
    DBConfigReader dbConfigReader;

    @Autowired
    MultiTenantDataSourceHolder multiTenantDataSourceHolder;

    @Autowired
    HDIDeployerClient hdiDeployerClient;

    private InstanceManagerUtil instanceManagerUtil = new InstanceManagerUtil();

    private Map<String, BlockingQueue<ManagedServiceInstance>> tenantAsyncResults = new ConcurrentHashMap<>();

    private static final Integer THREADS_NUMBERS = 10;

    public void initialize4OneTable(String tableName) throws Exception{
        List<String> tenantList = tenantHelper.getAllTenants(tableName);

        ExecuteInitilization(tenantList);

    }

    private void ExecuteInitilization(List<String> tenantList) throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tenantLatch = new CountDownLatch(tenantList.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);

        InstanceManagerClient imClient = instanceManagerUtil.getInstanceManagerClient();
        Long startTimestamp = System.currentTimeMillis();

        for (String tenantId : tenantList) {
            executorService.submit(() -> {
                DataSource dataSource = multiTenantDataSourceHolder.getDataSource(tenantId);

                if (dataSource == null) {
                    ManagedServiceInstance managedServiceInstance = null;

                    try {
                        managedServiceInstance = imClient.getManagedInstance(tenantId);
                    } catch (ImClientException e) {
                        e.printStackTrace();
                    }
                    if (managedServiceInstance == null) { //new tenant to be created
                        final BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();
                        tenantAsyncResults.put(tenantId, asyncResult);

                        Map<String, Object> provisioningParameters = new HashMap<>();
                        InstanceCreationOptions instanceCreationOptions = new InstanceCreationOptions();
                        instanceCreationOptions = instanceCreationOptions.withProvisioningParameters(provisioningParameters);
                        try {
                            imClient.createManagedInstance(tenantId, this.creationManagedInstanceCallback, instanceCreationOptions);
                        } catch (ImClientException e) {
                            throw new HanaDataSourceDeterminationException("[Initialization] Create managed instance failed", e);
                        }
                        try {
                            Object instanceManager = asyncResult.take();
                            managedServiceInstance = (ManagedServiceInstance) instanceManager;
                        } catch (InterruptedException e) {
                            log.warn("[Initialization] ManagedServiceInstance interrupted exception occurs:", e);
                            // Restore interrupted state...
                            Thread.currentThread().interrupt();
                        }
                    }
                    if (managedServiceInstance == null) {
                        throw new HanaDataSourceDeterminationException("[Initialization] Create managed instance failed for tenant: " + tenantId);
                    }
                    // call HDI deployer
                    try {
                        hdiDeployerClient.executeHDIDeployment(managedServiceInstance);
                        dataSource = hdiDeployerClient.createDataSource(managedServiceInstance);
                        if(managedServiceInstance.getId().equals(tenantId)) {
                            multiTenantDataSourceHolder.storeDataSource(tenantId, dataSource);
                        }
                    } catch (HDIDeploymentException e) {
                        hasError.set(true);
                        log.error("[Initialization] Determine data source failed", e);
                    }
                }

                tenantLatch.countDown();
            });
        }

        if(!tenantLatch.await(3000, TimeUnit.SECONDS)) {  // wait until latch counted down to 0
            log.error("[Initialization] Count down when time out: {}/{}", tenantLatch.getCount(), tenantList.size());
        }
        Long endTimestamp = System.currentTimeMillis();

        log.info("[Initialization] Initialize all tenants {}", (endTimestamp - startTimestamp));
        executorService.shutdown();

        if(hasError.get() || tenantLatch.getCount() != 0) {
            throw new HDIDeploymentInitializerException("[Initialization] error occurs, check log for details.");
        }

    }

    public void initialize4AllTables() throws Exception{
        List<String> tableList = dbConfigReader.getSourceTableNames();
        List<String> tenantList = new ArrayList<String>();
        Set<String> tenantSet = new HashSet<>();

        for (String tableName : tableList) {
            tenantList = tenantHelper.getAllTenants(tableName);
            tenantSet.addAll(tenantList);
        }
        List allTenants = new ArrayList(tenantSet);
        ExecuteInitilization(allTenants);

        log.info("[Initialization] ******* Initialize done for all tables." );
    }

    private InstanceManagerClient.CreationCallback creationManagedInstanceCallback = new InstanceManagerClient.CreationCallback() {
        @Override
        public void onCreationSuccess(ManagedServiceInstance managedServiceInstance) {
            log.info("[Initialization] Created managed instance for tenant: {}", managedServiceInstance.getId());
            BlockingQueue<ManagedServiceInstance> asyncResult = tenantAsyncResults.get(managedServiceInstance.getId());
            try {
                asyncResult.put(managedServiceInstance);
            } catch (InterruptedException e) {
                log.error("[Initialization][CreationSuccess] interrupted exception occurs:", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onCreationError(String s, String s1) {
            log.error("[Initialization] Create managed instance error: {} {}", s, s1);
            BlockingQueue<ManagedServiceInstance> asyncResult = tenantAsyncResults.get(s);
            try {
                asyncResult.put(null);
            } catch (InterruptedException e) {
                log.error("[Initialization][CreationError] interrupted exception occurs:", e);
                Thread.currentThread().interrupt();
            }
        }
    };

}
