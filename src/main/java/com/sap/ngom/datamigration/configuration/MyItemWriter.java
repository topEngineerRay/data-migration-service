package com.sap.ngom.datamigration.configuration;

import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyItemWriter implements ItemWriter<Map<String,Object>> {

    private DataSource dataSource;
    private String table;
    private String nameSpace;
    public MyItemWriter(DataSource dataSource, String table, String nameSpace) {
        this.dataSource = dataSource;
        this.table = table;
        this.nameSpace = nameSpace;
    }

    @Override
    public void write(List<? extends Map<String,Object>> list) throws Exception {
        if(list.isEmpty()){
            return;
        }
        //get all columns
        String[] columns = getColumns(list);
        SimpleJdbcInsert jdbcInsert = new SimpleJdbcInsert(dataSource)
                .usingColumns(columns)
                .withTableName(buildHanaTableName(nameSpace,table));

        list.forEach(row -> {
            jdbcInsert.execute(row);
        });

    }

    private String[] getColumns(List<? extends Map<String, Object>> list) {
        Map map = list.get(0);
        Set<String> set = map.keySet();
        //Does all the table have the same name convetion of tenant? This require a investigation
        set.remove("tenant_id");
        return set.toArray(new String[set.size()]);
    }

    private  String buildHanaTableName(String nameSpace,String table){
        return "\""+ nameSpace + "." + table+"\"";
    }
}