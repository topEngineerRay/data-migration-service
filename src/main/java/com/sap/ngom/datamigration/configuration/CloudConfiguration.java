package com.sap.ngom.datamigration.configuration;

import org.springframework.cloud.config.java.AbstractCloudConfig;
import org.springframework.cloud.service.PooledServiceConnectorConfig;
import org.springframework.cloud.service.relational.DataSourceConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Profile("cloud")
@Configuration
public class CloudConfiguration extends AbstractCloudConfig {

    /**
     * tomcat default (30,000ms)
     */
    private static final int MAX_WAIT_TIME_MS = 30000;

    /**
     * This replaces the definition of the DataSource in https://github.com/spring-cloud/spring-cloud-connectors/blob/eaa65c2/spring-cloud-spring-service-connector/src/main/java/org/springframework/cloud/service/relational/DbcpLikePooledDataSourceCreator.java,
     * which defines a JDBC connection pool of only 4.
     */
    @Bean
    public DataSource sourceDataSource() {
        final PooledServiceConnectorConfig.PoolConfig poolConfig = new PooledServiceConnectorConfig.PoolConfig(16, MAX_WAIT_TIME_MS);
        final DataSourceConfig dbConfig = new DataSourceConfig(poolConfig, null);
        final DataSource dataSource = connectionFactory().dataSource(dbConfig);
        return dataSource;
    }

}