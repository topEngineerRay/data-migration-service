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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Log4j2
public class ManagedInstanceService {
    private InstanceManagerUtil instanceManagerUtil = new InstanceManagerUtil();

    @Autowired
    MultiTenantDataSourceHolder multiTenantDataSourceHolder;

    public void deleteAll() throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(10);
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
                        imClient.deleteManagedInstance(msi.getId());
                        multiTenantDataSourceHolder.deleteDataSource(msi.getId());
                    } catch (ImClientException e) {
                        hasErrorCallHDIDeployer.set(true);
                    }
                }
                tenantLatch.countDown();
            });
        }

//        if(!tenantLatch.await(180, TimeUnit.SECONDS)) {  // wait until latch counted down to 0
//            log.info("Count down when time out: {}", tenantLatch.getCount());
//        }
        tenantLatch.await();
        Long endTimestamp = System.currentTimeMillis();

        log.info("Delete all takes Time: {}", (endTimestamp - startTimestamp));
        executorService.shutdown();

        if(hasErrorCallHDIDeployer.get() || tenantLatch.getCount() != 0) {
        }
    }
}
