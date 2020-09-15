package com.phani.demo.scheduler.exception;

public class UnknownTenantException extends Exception {
    public UnknownTenantException(String tenantId) {
        super("Unknown Tenant with tenant id: "+tenantId);
    }
}
