package com.sap.ngom.datamigration.configuration.hanaDBConfiguration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;

@Configuration
public class HANADataSourceConfiguration {

    @Bean
    public DataSource routingDataSource( ) {
        return new HANAMultiTenantRoutingDataSource( );
    }

    @Bean(name = "hanaRestTemplate")
    public RestTemplate hanaRestTemplate() {
        return new RestTemplate();
    }

}
