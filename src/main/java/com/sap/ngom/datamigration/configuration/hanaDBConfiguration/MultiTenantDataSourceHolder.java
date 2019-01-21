package com.sap.ngom.datamigration.configuration.hanaDBConfiguration;

import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 *  This is used for multi tenant data source cache.
 */
@Component
public class MultiTenantDataSourceHolder {
    private static Map<Object, DataSource> cachedDataSources = new HashMap<>();
    private static Map<String, String> cachedTenants = new HashMap<>();

    public static DataSource getDataSource(String tenantId) {
        return cachedDataSources.get(tenantId);
    }

    public static void storeDataSource(String tenantId, DataSource dataSource) {
        cachedDataSources.put(tenantId, dataSource);
    }

    public static void deleteDataSource(String tenantId) {
        cachedDataSources.remove(tenantId);
    }

    public static String getTenant(String tenantId) {
        return cachedTenants.get(tenantId);
    }

    public static void storeTenant(String tenantIdKey, String tenantIdValue) {
        cachedTenants.put(tenantIdKey, tenantIdValue);
    }

    public static void deleteTenant(String tenantIdKey) {
        cachedTenants.remove(tenantIdKey);
    }

}
