package com.sap.ngom.datamigration.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class SourceTableNotDefinedException extends RuntimeException{

    public SourceTableNotDefinedException(String message) {
        super(message);
    }
}
