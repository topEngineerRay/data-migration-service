package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.HDIDeployerUtil;
import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.MultiTenantDataSourceHolder;
import com.sap.xsa.core.instancemanager.client.InstanceCreationOptions;
import com.sap.xsa.core.instancemanager.client.InstanceManagerClient;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Component
public class HanaCreateTableUtil {

    public static String currentTenant = "";

    HDIDeployerUtil hdiDeployerUtil;

    @Autowired
    HanaCreateTableUtil( HDIDeployerUtil hdiDeployerUtil){
        this.hdiDeployerUtil = hdiDeployerUtil;
    }
    final  BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();

    public void createSchema(String tenantId) throws Exception{
        currentTenant = tenantId;
        //check if the data souce is already cached
        DataSource dataSource = MultiTenantDataSourceHolder.getDataSource(tenantId);
        if (dataSource == null) {
            InstanceManagerClient instanceManagerClient = InstanceManagerUtil.getInstanceManagerClient();
            ManagedServiceInstance managedServiceInstance = instanceManagerClient.getManagedInstance(tenantId);
            if (managedServiceInstance == null) { //new tenant to be created
                Map<String, Object> provisioningParameters = new HashMap<>();
                //provisioningParameters.put("database_id", ngomHeaderFacade.getUserId());
                InstanceCreationOptions instanceCreationOptions = new InstanceCreationOptions();
                instanceCreationOptions = instanceCreationOptions
                        .withProvisioningParameters(provisioningParameters);
                instanceManagerClient.createManagedInstance(tenantId, creationManagedInstanceCallback,
                        instanceCreationOptions);

                try {
                    Object instanceManager = asyncResult.take();
                    managedServiceInstance = (ManagedServiceInstance) instanceManager;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (managedServiceInstance == null) {
                throw new Exception("Error occurs when creating managed instance for tenant: " + tenantId);
            }

            // call HDI deployer
            //HDIDeployerUtil hdiDeployerUtil = new HDIDeployerUtil();
            if (hdiDeployerUtil.executeHDIDeployer(managedServiceInstance) != 0) { //failed
                throw new Exception("Error occurs when calling HDI Deployer for tenant: " + tenantId);
            } else {
                dataSource = hdiDeployerUtil.createDataSource(managedServiceInstance);
                MultiTenantDataSourceHolder.storeDataSource(tenantId, dataSource);
            }
        }
    }

    private InstanceManagerClient.CreationCallback creationManagedInstanceCallback = new InstanceManagerClient.CreationCallback() {
        @Override
        public void onCreationSuccess(ManagedServiceInstance managedServiceInstance) {
            try {
                asyncResult.put(managedServiceInstance);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onCreationError(String s, String s1) {

        }
    };
}
