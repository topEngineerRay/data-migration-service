package com.sap.ngom.datamigration.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;


@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class DataVerificationException extends RuntimeException {
    public DataVerificationException(String message) {
        super(message);
    }

    public DataVerificationException(String message, Exception e) {
        super(message,e);
    }

}

