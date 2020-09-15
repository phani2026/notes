package com.phani.demo.scheduler.impl;

import com.phani.demo.scheduler.MultiTenantWorkScheduler;
import com.phani.demo.scheduler.vo.TenantAwareTask;
import com.phani.demo.scheduler.vo.TenantConfig;

public class WeightedFairQueueingWorkScheduler extends MultiTenantWorkScheduler {
    @Override
    public void add(TenantAwareTask task) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TenantAwareTask remove() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void provisionTenant(TenantConfig tenantConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deProvisionTenant(String tenantId) {
        throw new UnsupportedOperationException();
    }
}
