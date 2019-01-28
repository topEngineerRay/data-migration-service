package com.sap.ngom.datamigration.configuration;

import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class MyItemWriter implements ItemWriter<Map<String,Object>> {

    private DataSource dataSource;
    private String table;

    public MyItemWriter(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        this.table = table;
    }

    @Override
    public void write(List<? extends Map<String,Object>> list) throws Exception {
        SimpleJdbcInsert jdbcInsert = new SimpleJdbcInsert(dataSource).withTableName("com.sap.ngom.db::BusinessPartner." + table);

        list.forEach(row -> {
            jdbcInsert.execute(row);
        });

    }
}