package com.sap.ngom.datamigration.exception;

public class SourceTableNotDefinedException extends RuntimeException{

    public SourceTableNotDefinedException(String message) {
        super(message);
    }
}
