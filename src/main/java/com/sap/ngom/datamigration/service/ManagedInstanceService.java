package com.sap.ngom.datamigration.service;

import com.sap.ngom.datamigration.util.InstanceManagerUtil;
import com.sap.ngom.util.hana.db.MultiTenantDataSourceHolder;
import com.sap.xsa.core.instancemanager.client.OperationStatus;
import lombok.extern.log4j.Log4j2;
import com.sap.xsa.core.instancemanager.client.ImClientException;
import com.sap.xsa.core.instancemanager.client.InstanceManagerClient;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Log4j2
public class ManagedInstanceService {
    private InstanceManagerUtil instanceManagerUtil = new InstanceManagerUtil();

    @Autowired
    MultiTenantDataSourceHolder multiTenantDataSourceHolder;
    private Map<String, BlockingQueue<Boolean>> tenantAsyncResults = new ConcurrentHashMap<>();

    public void deleteAll() throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        InstanceManagerClient imClient = instanceManagerUtil.getInstanceManagerClient();
        List<ManagedServiceInstance> managedServiceInstances = imClient.getManagedInstances();

        Long startTimestamp = System.currentTimeMillis();

        CountDownLatch tenantLatch = new CountDownLatch(managedServiceInstances.size());
        final AtomicBoolean hasErrorCallHDIDeployer = new AtomicBoolean(false);

        for (ManagedServiceInstance msi : managedServiceInstances) {
            executorService.submit(() -> {
                if(msi.getId().startsWith("15") || msi.getId().startsWith("2018") ||
                        msi.getId().startsWith("ContactName") || msi.getId().startsWith("CustomRef") ||
                        msi.getId().startsWith("CustomerName") || msi.getId().startsWith("PubCustomRef") ||
                        msi.getId().startsWith("UICustomRef") || msi.getStatus() == OperationStatus.CREATION_FAILED ||
                        msi.getStatus() == OperationStatus.DELETION_FAILED) {
                    try {
                        final BlockingQueue<Boolean> asyncResult = new SynchronousQueue<>();
                        tenantAsyncResults.put(msi.getId(), asyncResult);

                        imClient.deleteManagedInstance(msi.getId(), this.deletionManagedInstanceCallback);
                        Object asynIsDeleted = asyncResult.take();
                        Boolean isDeleted = (Boolean) asynIsDeleted;
                        if (isDeleted) {
                            multiTenantDataSourceHolder.deleteDataSource(msi.getId());
                        } else {
                            hasErrorCallHDIDeployer.set(true);
                        }
                    } catch (ImClientException e) {
                        hasErrorCallHDIDeployer.set(true);
                    } catch (InterruptedException e) {
                        log.error("[Managed Instance Cleanup] Exception interrupted exception occurs: ", e);
                    }
                }
                tenantLatch.countDown();
            });
        }

        tenantLatch.await();
        Long endTimestamp = System.currentTimeMillis();

        log.info("Delete all takes Time: {}", (endTimestamp - startTimestamp));
        executorService.shutdownNow();

        if(hasErrorCallHDIDeployer.get()) {
            log.error("[Managed Instance Cleanup] Deletion failed.");
        }
    }

    private InstanceManagerClient.DeletionCallback deletionManagedInstanceCallback = new InstanceManagerClient.DeletionCallback() {
        @Override
        public void onDeletionSuccess(String tenantId) {
            log.info("[Managed Instance Cleanup] Deleted managed instance for tenant: {}", tenantId);
            BlockingQueue<Boolean> asyncResult = tenantAsyncResults.get(tenantId);
            try {
                asyncResult.put(true);
            } catch (InterruptedException e) {
                log.error("[Managed Instance Cleanup] Exception managed service onDeletionSuccess interrupted exception occurs:", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onDeletionError(String s, String s1) {
            log.error("[Managed Instance Cleanup] Delete managed instance error: {} {}", s, s1);
            BlockingQueue<Boolean> asyncResult = tenantAsyncResults.get(s);
            try {
                asyncResult.put(false);
            } catch (InterruptedException e) {
                log.error("[Managed Instance Cleanup] Exception managed service onDeletionError interrupted exception occurs:", e);
                Thread.currentThread().interrupt();
            }
        }
    };

}
