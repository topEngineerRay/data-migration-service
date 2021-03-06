package com.sap.ngom.datamigration.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class InitializerException extends RuntimeException {
    public InitializerException(String message) {
        super(message);
    }
}

