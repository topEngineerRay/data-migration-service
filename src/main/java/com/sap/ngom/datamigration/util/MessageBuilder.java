package com.sap.ngom.datamigration.util;

public class MessageBuilder {
    private String status;
    private String message;

    public void setMessage(String message) {
        this.message = message;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public String getStatus() {
        return status;
    }
}
