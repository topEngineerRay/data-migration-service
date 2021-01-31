package com.sap.ngom.datamigration.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CustomItemProcessor implements ItemProcessor<Map<String,Object>, Map<String,Object>> {

    @Override
    public Map process(Map object) throws Exception {
        return object; //do nothing now
    }

}
