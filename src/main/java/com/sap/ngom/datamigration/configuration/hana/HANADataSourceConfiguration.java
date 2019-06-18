package com.sap.ngom.datamigration.configuration.hana;

import com.sap.ngom.util.hana.db.MultiTenantDataSourceHolder;
import com.sap.ngom.util.hana.db.utils.HDIDeployerClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;

@Configuration("data-migration-hana-data-source-configuration")
public class HANADataSourceConfiguration {

    @Bean("targetDataSource")
    public DataSource routingDataSource( HDIDeployerClient hdiDeployerClient,
                                         MultiTenantDataSourceHolder multiTenantDataSourceHolder) {
        return new TenantSpecificHANAMultitRoutingDataSource(hdiDeployerClient, multiTenantDataSourceHolder);
    }

    @Bean(name = "hanaRestTemplate")
    public RestTemplate hanaRestTemplate() {
        return new RestTemplate();
    }

}
