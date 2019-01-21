package com.sap.ngom.datamigration.mapper.RowMapper;

import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Map;

public class MapItemSqlParameterSourceProvider implements
        ItemSqlParameterSourceProvider<Map<String, Object>> {

    public SqlParameterSource createSqlParameterSource(Map<String, Object> item) {
        return new MapSqlParameterSource(item);
    }

}