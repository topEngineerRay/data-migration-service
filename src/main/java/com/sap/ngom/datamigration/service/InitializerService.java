package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.exception.InitializerException;
import com.sap.ngom.datamigration.util.DBConfigReader;
import com.sap.ngom.datamigration.util.InstanceManagerUtil;
import com.sap.ngom.datamigration.util.TableNameValidator;
import com.sap.ngom.datamigration.util.TenantHelper;
import com.sap.ngom.util.hana.db.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.exceptions.HDIDeploymentException;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;
import com.sap.xsa.core.instancemanager.client.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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

    @Autowired
    private TableNameValidator tableNameValidator;

    private InstanceManagerUtil instanceManagerUtil = new InstanceManagerUtil();

    private Map<String, BlockingQueue<ManagedServiceInstance>> tenantAsyncResults = new ConcurrentHashMap<>();

    private static final Integer THREADS_NUMBERS = 10;

    public void initialize4OneTable(String tableName){
        tableNameValidator.tableNameValidation(tableName);
        List<String> tenantList = tenantHelper.getAllTenants(tableName);
        ExecuteInitilization(tenantList);

    }

    private void ExecuteInitilization(List<String> tenantList){
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS_NUMBERS);
        CountDownLatch tenantLatch = new CountDownLatch(tenantList.size());
        final AtomicBoolean hasError = new AtomicBoolean(false);

        InstanceManagerClient imClient = null;
        try {
            imClient = instanceManagerUtil.getInstanceManagerClient();
        } catch (Exception e) {
            throw new InitializerException(e.getMessage());
        }
        Long startTimestamp = System.currentTimeMillis();

        InstanceManagerClient finalImClient = imClient;

        for (String tenantId : tenantList) {
            String oriTenantId = null;
            try {
                oriTenantId = URLEncoder.encode(tenantId, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new InitializerException(e.getMessage());
            }
            String finalTenantId = oriTenantId;
            executorService.submit(() -> {
                try {
                    DataSource dataSource = multiTenantDataSourceHolder.getDataSource(finalTenantId);

                    if (dataSource == null) {
                        ManagedServiceInstance managedServiceInstance = finalImClient.getManagedInstance(finalTenantId);

                        if (managedServiceInstance == null) { //new tenant to be created
                            final BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();
                            tenantAsyncResults.put(finalTenantId, asyncResult);

                            Map<String, Object> provisioningParameters = new HashMap<>();
                            InstanceCreationOptions instanceCreationOptions = new InstanceCreationOptions();
                            instanceCreationOptions = instanceCreationOptions.withProvisioningParameters(provisioningParameters);

                            finalImClient.createManagedInstance(finalTenantId, this.creationManagedInstanceCallback, instanceCreationOptions);
                            Object instanceManager = asyncResult.take();
                            managedServiceInstance = (ManagedServiceInstance) instanceManager;
                        }

                        if (managedServiceInstance == null) {
                            hasError.set(true);
                            log.error("[Initialization] Exception when create managed instance for tenant: " + finalTenantId);
                            tenantLatch.countDown();
                            Thread.currentThread().interrupt();
                        }

                        if (managedServiceInstance.getStatus() != OperationStatus.CREATION_SUCCEEDED && managedServiceInstance.getStatus() != OperationStatus.UPDATE_SUCCEEDED) {
                            hasError.set(true);
                            log.error("[Initialization] Exception the managed hana instance status is " + managedServiceInstance.getStatus() + " for Tenant: " + managedServiceInstance.getId());
                        } else {
                            // call HDI deployer
                            hdiDeployerClient.executeHDIDeployment(managedServiceInstance);
                            dataSource = hdiDeployerClient.createDataSource(managedServiceInstance);
                            if (managedServiceInstance.getId().equals(finalTenantId)) {
                                multiTenantDataSourceHolder.storeDataSource(finalTenantId, dataSource);
                            }
                        }
                    }
                } catch (ImClientException e) {
                    log.error("[Initialization] Exception when get/create managed instance: ", e);
                } catch (InterruptedException e) {
                    log.error("[Initialization] Exception interrupted exception occurs: ", e);
                } catch (HDIDeploymentException e) {
                    hasError.set(true);
                    log.error("[Initialization] Exception when call HDI deployer: " + e.getMessage(), e);
                } finally {
                    tenantLatch.countDown();
                }

                log.info("[Initialization] Initialization done for tenant: " + finalTenantId + ". " + tenantLatch.getCount() + "/" + tenantList.size());
            });
        }

        try {
            if(!tenantLatch.await(3000, TimeUnit.SECONDS)) {  // wait until latch counted down to 0
                log.error("[Initialization] Timeout count down: {}/{}", tenantLatch.getCount(), tenantList.size());
            }
        } catch (InterruptedException e) {
            log.error("[Initialization] Exception tenantLatch.await interrupted exception occurs:", e);
        }
        executorService.shutdownNow();

        if(tenantLatch.getCount() != 0) {
            throw new InitializerException("[Initialization] Timeout after 50 mins");
        }
        if(hasError.get()) {
            throw new InitializerException("[Initialization] Error occurs when initialize tenants. Search logs with keyword '[Initialization] Exception'");
        }
        Long endTimestamp = System.currentTimeMillis();
        log.info("[Initialization] Initialize all tenants, takes time {}", (endTimestamp - startTimestamp));
    }

    public void initialize4AllTables() {
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
                log.error("[Initialization] Exception managed service onCreationSuccess interrupted exception occurs:", e);
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
                log.error("[Initialization] Exception managed service onCreationError interrupted exception occurs:", e);
                Thread.currentThread().interrupt();
            }
        }
    };

}
