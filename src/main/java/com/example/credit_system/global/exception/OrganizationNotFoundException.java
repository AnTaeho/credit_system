package com.example.credit_system.global.exception;

public class OrganizationNotFoundException extends RuntimeException {

    public OrganizationNotFoundException(Long organizationId) {
        super("존재하지 않는 organization: " + organizationId);
    }
}
