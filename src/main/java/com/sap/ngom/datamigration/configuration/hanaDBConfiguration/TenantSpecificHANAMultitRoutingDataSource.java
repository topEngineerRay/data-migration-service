package com.sap.ngom.datamigration.configuration.hanaDBConfiguration;

import com.sap.ngom.util.hana.db.configuration.HANAMultiTenantRoutingDataSource;
import com.sap.ngom.util.hana.db.configuration.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;
import com.sap.ngom.util.headers.NgomHeaderFacade;

import java.util.HashMap;
import java.util.Map;

public class TenantSpecificHANAMultitRoutingDataSource extends HANAMultiTenantRoutingDataSource {

    private static ThreadLocal<String> CONTEXT
            = new ThreadLocal<>();

    TenantSpecificHANAMultitRoutingDataSource(HDIDeployerClient hdiDeployerClient,
                                              MultiTenantDataSourceHolder multiTenantDataSourceHolder,
                                              NgomHeaderFacade ngomHeaderFacade){
        super(hdiDeployerClient,multiTenantDataSourceHolder,ngomHeaderFacade);
    }

    public static void setTenant(String tenantId){
        CONTEXT.set(tenantId);
    }

    @Override
    protected String getTenantId() {
        return CONTEXT.get();
    }
}
