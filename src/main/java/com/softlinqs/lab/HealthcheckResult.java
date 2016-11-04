package com.softlinqs.lab;

public class HealthcheckResult {

    private boolean isHealthy;
    private String message;

    public HealthcheckResult(boolean isHealthy, String message) {
        this.isHealthy = isHealthy;
        this.message = message;
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public String getMessage() {
        return message;
    }
}
