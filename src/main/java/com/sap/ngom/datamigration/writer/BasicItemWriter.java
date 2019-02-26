package com.sap.ngom.datamigration.writer;

import org.springframework.batch.item.ItemWriter;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicItemWriter implements ItemWriter<Map<String,Object>> {
    protected DataSource dataSource;
    protected String table;
    protected String nameSpace;
    public BasicItemWriter(DataSource dataSource, String table, String nameSpace) {
        this.dataSource = dataSource;
        this.table = table;
        this.nameSpace = nameSpace;
    }

    protected String[] getColumns(List<? extends Map<String, Object>> list) {
        Map map = list.get(0);
        Set<String> set = map.keySet();
        set.remove("tenant_id");
        set.remove(("tenant"));
        return set.toArray(new String[set.size()]);
    }

    protected  String buildHanaTableName(String nameSpace,String table){
        return "\""+ nameSpace + "." + table+"\"";
    }

    @Override public void write(List<? extends Map<String, Object>> items) throws Exception {

    }
}
