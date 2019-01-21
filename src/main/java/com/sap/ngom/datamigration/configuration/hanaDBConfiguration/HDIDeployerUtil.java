package com.sap.ngom.datamigration.configuration.hanaDBConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.xsa.core.instancemanager.client.ManagedServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class HDIDeployerUtil {
    @Value("${HDIDeployer.url}")
    private String hdiDeployerUrl;

    @Value("${HDIDeployer.security.username}")
    private String user;

    @Value("${HDIDeployer.security.password}")
    private String password;

    @Autowired
    @Qualifier("hanaRestTemplate")
    protected RestTemplate restTemplateHANA;

    public Integer executeHDIDeployer(ManagedServiceInstance managedServiceInstance) {
        Integer exitCode = -1;
        Map credentials = managedServiceInstance.getCredentials();

        Map body = new HashMap();
        body.put("id", managedServiceInstance.getId());
        body.put("status", managedServiceInstance.getStatus());
        body.put("credentials", credentials);
//        System.out.println("body: " + body);

        System.out.println("before call HDI deployer tenant:" + managedServiceInstance.getId());
        restTemplateHANA.getInterceptors().add(new BasicAuthorizationInterceptor(user, password));
        String resStr = restTemplateHANA.postForObject(hdiDeployerUrl, body, String.class);
        ObjectMapper mapper = new ObjectMapper();
        try{
            System.out.println("after call HDI deployer tenant:" + managedServiceInstance.getId());
            Map res = mapper.readValue(resStr, Map.class);
            exitCode = (Integer)res.get("exitCode");
            if(exitCode != 0) {
                System.out.println("Call HDI Deployer failed for tenant " + managedServiceInstance.getId() + " Response: " + res);
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
        return exitCode;
    }

    public DataSource createDataSource(ManagedServiceInstance managedServiceInstance) {
        Map credentials = managedServiceInstance.getCredentials();
        String url = (String) credentials.get("url");
        String user = (String) credentials.get("user");
        String pass = (String) credentials.get("password");

        return DataSourceBuilder.create()
                .url(url)
                .driverClassName("com.sap.db.jdbc.Driver")
                .username(user)
                .password(pass)
                .build();
    }
}
