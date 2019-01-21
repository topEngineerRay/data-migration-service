package com.sap.ngom.datamigration.configuration.hanaDBConfiguration;

import com.sap.ngom.datamigration.util.HanaCreateTableUtil;
import com.sap.ngom.datamigration.util.InstanceManagerUtil;
import com.sap.xsa.core.instancemanager.client.InstanceCreationOptions;
import com.sap.xsa.core.instancemanager.client.InstanceManagerClient;
import com.sap.xsa.core.instancemanager.client.InstanceManagerClient.CreationCallback;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.AbstractDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public class HANAMultiTenantRoutingDataSource extends AbstractDataSource {

    @Autowired
    HDIDeployerUtil hdiDeployerUtil;

    private String currentMigrationTenant = "";
    /*@Autowired
    NgomHeaderFacade ngomHeaderFacade;*/

    final BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();
    private CreationCallback creationManagedInstanceCallback = new CreationCallback() {
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

    @Override
    public Connection getConnection() throws SQLException {
        DataSource targetDataSource = null;
        try {
            targetDataSource = this.determineTargetDataSource();
        }catch (Exception e) { //TODO error handling
            e.printStackTrace();
        }

        if(targetDataSource != null) {
            return targetDataSource.getConnection();
        }
        return null;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        DataSource targetDataSource = null;
        try {
            targetDataSource = this.determineTargetDataSource();
        }catch (Exception e) { //TODO error handling
            e.printStackTrace();
        }

        if(targetDataSource != null) {
            return targetDataSource.getConnection(username, password);
        }
        return null;
    }

    protected DataSource determineTargetDataSource() throws Exception{
        currentMigrationTenant = HanaCreateTableUtil.currentTenant;

        if(currentMigrationTenant == "") {
            currentMigrationTenant = "ngom-admin-reserved";   // default
        }
        DataSource dataSource = MultiTenantDataSourceHolder.getDataSource(currentMigrationTenant);
        if (dataSource == null) {
            InstanceManagerClient instanceManagerClient = InstanceManagerUtil.getInstanceManagerClient();
            ManagedServiceInstance managedServiceInstance = instanceManagerClient.getManagedInstance(currentMigrationTenant);
            if(managedServiceInstance == null) { //new tenant to be created
                Map<String, Object> provisioningParameters = new HashMap<>();
                //provisioningParameters.put("database_id", ngomHeaderFacade.getUserId());
                InstanceCreationOptions instanceCreationOptions = new InstanceCreationOptions();
                instanceCreationOptions = instanceCreationOptions.withProvisioningParameters(provisioningParameters);
                instanceManagerClient.createManagedInstance(currentMigrationTenant, this.creationManagedInstanceCallback, instanceCreationOptions);

                try {
                    Object instanceManager = asyncResult.take();
                    managedServiceInstance = (ManagedServiceInstance) instanceManager;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(managedServiceInstance == null) {
                throw new Exception("Error occurs when creating managed instance for tenant: " + currentMigrationTenant);
            }

            // call HDI deployer
            //HDIDeployerUtil hdiDeployerUtil = new HDIDeployerUtil();
            if(hdiDeployerUtil.executeHDIDeployer(managedServiceInstance) != 0) { //failed
                throw new Exception("Error occurs when calling HDI Deployer for tenant: " + currentMigrationTenant);
            } else {
                dataSource = hdiDeployerUtil.createDataSource(managedServiceInstance);
                MultiTenantDataSourceHolder.storeDataSource(currentMigrationTenant, dataSource);
            }
        }
        return dataSource;
    }


}
