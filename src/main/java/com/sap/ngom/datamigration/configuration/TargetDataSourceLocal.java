package com.sap.ngom.datamigration.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Configuration
public class TargetDataSourceLocal {

    @Bean("targetDataSource")
    @ConfigurationProperties(prefix="spring.datasource.target")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

}
