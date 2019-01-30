package com.sap.ngom.datamigration.exception;

public class NoJobExistForGivenTableException extends RuntimeException{
    public NoJobExistForGivenTableException(String message) {
        super(message);
    }
}
