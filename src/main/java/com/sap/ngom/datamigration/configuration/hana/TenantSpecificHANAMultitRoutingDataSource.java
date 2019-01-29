package com.sap.ngom.datamigration.configuration.hana;

import com.sap.ngom.util.hana.db.configuration.HANAMultiTenantRoutingDataSource;
import com.sap.ngom.util.hana.db.configuration.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;

public class TenantSpecificHANAMultitRoutingDataSource extends HANAMultiTenantRoutingDataSource {

    private static ThreadLocal<String> CONTEXT = new ThreadLocal<>();

    TenantSpecificHANAMultitRoutingDataSource(HDIDeployerClient hdiDeployerClient,
                                              MultiTenantDataSourceHolder multiTenantDataSourceHolder){
        super(hdiDeployerClient, multiTenantDataSourceHolder);
    }

    public static void setTenant(String tenantId){
        CONTEXT.set(tenantId);
    }

    @Override
    protected String getTenantId() {
        return CONTEXT.get();
    }
}
