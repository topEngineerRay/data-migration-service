package com.sap.ngom.datamigration.processor;

import com.sap.ngom.datamigration.configuration.hanaDBConfiguration.MultiTenantDataSourceHolder;
import com.sap.ngom.datamigration.util.HanaCreateTableUtil;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Component
public class BPItemProcessor implements ItemProcessor<Map<String,Object>, Map<String,Object>> {

    @Autowired
    @Qualifier("routingDataSource")
    DataSource detinationDataSource;

    @Autowired
    HanaCreateTableUtil hanaCreateTableUtil;

    final BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();

    @Override
    public Map process(Map bp) throws Exception {

        if(null == MultiTenantDataSourceHolder.getTenant(bp.get("tenant_id").toString())) {
            MultiTenantDataSourceHolder.storeTenant(bp.get("tenant_id").toString(),bp.get("tenant_id").toString());
            hanaCreateTableUtil.createSchema(bp.get("tenant_id").toString());
        }
        return bp;//do nothing now
    }

}
