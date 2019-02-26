package com.sap.ngom.datamigration.writer;

import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpecificRecordItemWriter implements ItemWriter<Map<String,Object>> {

    private DataSource dataSource;
    private String table;
    private String nameSpace;

    public SpecificRecordItemWriter(DataSource dataSource, String table, String nameSpace) {
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
        //when there is a duplicated key exception, method execute and exectueBatch will throw different exception
        list.forEach(item ->{
            try {
                jdbcInsert.execute(item);
            }catch(Exception exception){
                if(exception.getMessage().contains("unique constraint violated")){
                    throw new DuplicateKeyException(exception.getMessage());
                }else {
                    throw exception;
                }
            }
        });
    }

    private String[] getColumns(List<? extends Map<String, Object>> list) {
        Map map = list.get(0);
        Set<String> set = map.keySet();
        set.remove("tenant_id");
        return set.toArray(new String[set.size()]);
    }

    private  String buildHanaTableName(String nameSpace,String table){
        return "\""+ nameSpace + "." + table+"\"";
    }
}