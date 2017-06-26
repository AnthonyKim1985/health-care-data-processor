package org.bigdatacenter.dataprocessor.springboot.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
public class RestException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(RestException.class);
    private final String currentThreadName = Thread.currentThread().getName();

    public RestException(String message) {
        super(message);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
    }

    public RestException(Throwable cause) {
        super(cause);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, cause.getMessage()));
    }

    public RestException(String message, Throwable cause) {
        super(message, cause);
        logger.error(String.format("%s - REST Exception occurs: %s, caused: %s", currentThreadName, message, cause.getMessage()));
    }

    public RestException(String message, HttpServletResponse httpServletResponse) {
        super(message);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
}