package org.bigdatacenter.dataprocessor.springboot.handler;

import org.bigdatacenter.dataprocessor.springboot.exception.RestException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by hyuk0 on 2017-06-02.
 */
@RestController
@ControllerAdvice
public class GlobalRestExceptionHandler {
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = RestException.class)
    public String handleBaseException(RestException e) {
        return e.getMessage();
    }

    @ExceptionHandler(value = Exception.class)
    public String handleException(Exception e) {
        return e.getMessage();
    }
}