package com.sap.ngom.datamigration.writer;

import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcInsertOperations;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class GenericItemWriter extends BasicItemWriter {

    public GenericItemWriter(DataSource dataSource, String table, String nameSpace) {
        super(dataSource, table, nameSpace);
    }

    @Override
    public void write(List<? extends Map<String,Object>> list) {
        if(list.isEmpty()){
            return;
        }
        //get all columns
        String[] columns = getColumns(list);
        SimpleJdbcInsertOperations jdbcInsert = new SimpleJdbcInsert(dataSource)
                .usingColumns(columns)
                .withTableName(buildHanaTableName(nameSpace,table))
                .withoutTableColumnMetaDataAccess();

        jdbcInsert.executeBatch(list.toArray(new Map[0]));
    }

}