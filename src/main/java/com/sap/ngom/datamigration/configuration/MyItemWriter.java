package com.sap.ngom.datamigration.configuration;

import com.sap.ngom.datamigration.mapper.RowMapper.MapItemSqlParameterSourceProvider;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyItemWriter implements ItemWriter<Map<String,Object>> {

    private DataSource dataSource;
    private String table;

    public MyItemWriter(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
    }

    @Override
    public void write(List<? extends Map<String,Object>> list) throws Exception {
        if(list.isEmpty()){
            return;
        }
        //get all columns
        Map map = list.get(0);
        Set<String> set = map.keySet();
        set.remove("tenant_id");
        String[] columns = set.toArray(new String[set.size()]);

        SimpleJdbcInsert jdbcInsert = new SimpleJdbcInsert(dataSource)
                .usingColumns(columns)
                .withTableName("\"com.sap.ngom.db::BusinessPartner." + table+"\"");

        list.forEach(row -> {
            jdbcInsert.execute(row);
        });

    }
}