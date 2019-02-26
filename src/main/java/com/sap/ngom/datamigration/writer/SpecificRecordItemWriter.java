package com.sap.ngom.datamigration.writer;

import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpecificRecordItemWriter extends BasicItemWriter {

    public SpecificRecordItemWriter(DataSource dataSource, String table, String nameSpace) {
        super(dataSource, table, nameSpace);
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
                jdbcInsert.execute(item);
        });
    }


}