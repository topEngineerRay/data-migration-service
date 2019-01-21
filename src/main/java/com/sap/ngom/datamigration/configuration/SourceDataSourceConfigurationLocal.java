package com.sap.ngom.datamigration.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class SourceDataSourceConfigurationLocal {
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSource sourceDataSource() {
        return DataSourceBuilder.create().build();
    }

}
