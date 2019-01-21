package com.sap.ngom.datamigration.util;

import com.sap.xsa.core.instancemanager.client.InstanceManagerClient;
import com.sap.xsa.core.instancemanager.client.ServiceInstance;
import com.sap.xsa.core.instancemanager.client.impl.InstanceManagerClientFactory;

import java.util.LinkedList;
import java.util.List;

public class InstanceManagerUtil {
    private static InstanceManagerClient instanceManagerClient;

    public static InstanceManagerClient getInstanceManagerClient() throws Exception{
        if(instanceManagerClient == null) {
            List<ServiceInstance> serviceInstances = new LinkedList<>();
            String vcapServices = (String) System.getenv().get("VCAP_SERVICES");
            if (vcapServices != null && !vcapServices.isEmpty()) {
                serviceInstances = InstanceManagerClientFactory.getServicesFromVCAP(vcapServices);
            }
            if (serviceInstances.size() > 0) {
                ServiceInstance serviceInstance = serviceInstances.get(0);
                instanceManagerClient = InstanceManagerClientFactory.getInstance(serviceInstance);
                //TODO: > 1 exception
            } else {
                throw new Exception("No instance manager service instance bound to application");
            }
        }
        return instanceManagerClient;
    }
}
