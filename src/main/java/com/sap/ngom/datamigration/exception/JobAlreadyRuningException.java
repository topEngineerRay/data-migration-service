package com.sap.ngom.datamigration.exception;

public class JobAlreadyRuningException extends RuntimeException{
    public JobAlreadyRuningException(String message) {
        super(message);
    }
}
