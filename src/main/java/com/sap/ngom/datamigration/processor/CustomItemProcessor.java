package com.sap.ngom.datamigration.processor;

import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Component
public class CustomItemProcessor implements ItemProcessor<Map<String,Object>, Map<String,Object>> {

    @Autowired
    @Qualifier("routingDataSource")
    DataSource detinationDataSource;


    final BlockingQueue<ManagedServiceInstance> asyncResult = new SynchronousQueue<>();

    @Override
    public Map process(Map object) throws Exception {

        return object;//do nothing now
    }

}
