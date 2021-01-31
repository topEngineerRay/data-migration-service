package com.sap.ngom.datamigration.configuration.hana;

public class TenantThreadLocalHolder {
    private static final ThreadLocal<String> TENANT_CONTEXT = new ThreadLocal<>();

    public static void setTenant(String tenantId){
        TENANT_CONTEXT.set(tenantId);
    }

    public static String getTenant(){
        return TENANT_CONTEXT.get();
    }
}
