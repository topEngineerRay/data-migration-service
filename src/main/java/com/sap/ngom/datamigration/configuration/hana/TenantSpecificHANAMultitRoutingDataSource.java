package com.sap.ngom.datamigration.configuration.hana;

import com.sap.ngom.util.hana.db.HANAMultiTenantRoutingDataSource;
import com.sap.ngom.util.hana.db.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;

public class TenantSpecificHANAMultitRoutingDataSource extends HANAMultiTenantRoutingDataSource {

    TenantSpecificHANAMultitRoutingDataSource(HDIDeployerClient hdiDeployerClient,
                                              MultiTenantDataSourceHolder multiTenantDataSourceHolder){
        super(hdiDeployerClient, multiTenantDataSourceHolder);
    }

    @Override
    protected String getTenantId() {
        return TenantThreadLocalHolder.getTenant();
    }
}
