package com.sap.ngom.datamigration.configuration;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class SpringBatchMetadataConfiguration {

    @Bean("batchConfigDataSource")
    @Primary
    public DataSource batchConfigH2DataSource() {
        return DataSourceBuilder.create().url("jdbc:h2:mem:testdb").driverClassName("org.h2.Driver").username("sa").password("").build();
    }
}
