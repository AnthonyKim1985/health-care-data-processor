package org.bigdatacenter.dataprocessor.springboot.exception;

import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.bigdatacenter.dataprocessor.springboot.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletResponse;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
public class RestException extends RuntimeException {
    private static final Logger logger = LoggerFactory.getLogger(RestException.class);
    private final String currentThreadName = Thread.currentThread().getName();

    //@Autowired
    //private MetadbService metadbService;

    public RestException(Integer dataSetUID, String message) {
        super(message);
        //metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
    }

    public RestException(Integer dataSetUID, Throwable cause) {
        super(cause);
        //metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, cause.getMessage()));
    }

    public RestException(Integer dataSetUID, String message, Throwable cause) {
        super(message, cause);
        //metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
        logger.error(String.format("%s - REST Exception occurs: %s, caused: %s", currentThreadName, message, cause.getMessage()));
    }

    public RestException(String message, HttpServletResponse httpServletResponse) {
        super(message);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }

    public RestException(Integer dataSetUID, String message, HttpServletResponse httpServletResponse) {
        super(message);
        //metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
        logger.error(String.format("%s - REST Exception occurs: %s", currentThreadName, message));
        httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
}