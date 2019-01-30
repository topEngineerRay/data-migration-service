package com.sap.ngom.datamigration.exception.handler;

import com.sap.ngom.datamigration.exception.NoJobExistForGivenTableException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApplicationCommonExceptionHandler {

    @ExceptionHandler(NoJobExistForGivenTableException.class)
    @ResponseBody
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handleNoJobExistForGivenTableException (final NoJobExistForGivenTableException exception){
        return exception.getMessage();
    }
}
