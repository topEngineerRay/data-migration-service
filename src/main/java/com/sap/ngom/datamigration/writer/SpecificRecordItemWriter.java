package com.sap.ngom.datamigration.writer;

import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcInsertOperations;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

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
        SimpleJdbcInsertOperations jdbcInsert = new SimpleJdbcInsert(dataSource)
                .usingColumns(columns)
                .withTableName(buildHanaTableName(nameSpace,table))
                .withoutTableColumnMetaDataAccess();

        //when there is a duplicated key exception, method execute and exectueBatch will throw different exception
        list.forEach(item ->{
                jdbcInsert.execute(item);
        });
    }


}