package com.sap.ngom.datamigration.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Configuration
@Profile("postgres")
public class SourceDataSourceConfigurationLocal {
    @Bean
    @ConfigurationProperties("spring.datasource")
    public DataSource sourceDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean("batchConfigDataSource")
    @Primary
    public DataSource batchConfigH2DataSource() {
        return DataSourceBuilder.create().url("jdbc:h2:mem:testdb").driverClassName("org.h2.Driver").username("sa").password("").build();
    }
}
