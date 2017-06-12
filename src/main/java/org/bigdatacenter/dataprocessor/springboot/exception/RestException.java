package org.bigdatacenter.dataprocessor.springboot.exception;

/**
 * Created by hyuk0 on 2017-06-02.
 */
public class RestException extends RuntimeException {
    public RestException(String message) {
        super(message);
    }

    public RestException(Throwable cause) {
        super(cause);
    }

    public RestException(String message, Throwable cause) {
        super(message, cause);
    }
}