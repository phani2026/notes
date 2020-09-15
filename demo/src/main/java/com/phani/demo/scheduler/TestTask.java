package com.phani.demo.scheduler;

import com.phani.demo.scheduler.vo.SchedulingStrategy;
import com.phani.demo.scheduler.vo.TenantConfig;

import java.util.ArrayList;
import java.util.List;

public class TestTask {

    static int noOfTenants = 60;
    private static List<String> tenantIds;

    private static MultiTenantWorkQueueManager queueManager;


    public static void main(String[] args) {
        queueManager = new MultiTenantWorkQueueManager(createTenantConfigs(),
                SchedulingStrategy.FAIR_QUEUEING);

    }

    private static List<TenantConfig> createTenantConfigs() {
        List<TenantConfig> tenantConfigList = new ArrayList<>();
        tenantIds = new ArrayList<>();
        for (int i = 0; i < noOfTenants; i++) {
            String tenantId = "tenantId:"+i;
            TenantConfig cfg = new TenantConfig(tenantId, "tenantName:"+i, 10);
            tenantIds.add(tenantId);
            tenantConfigList.add(cfg);
        }
        return tenantConfigList;
    }


}
