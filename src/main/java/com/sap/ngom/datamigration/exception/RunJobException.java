package com.sap.ngom.datamigration.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class RunJobException extends RuntimeException {
    public RunJobException(String message) {
        super(message);
    }
}

