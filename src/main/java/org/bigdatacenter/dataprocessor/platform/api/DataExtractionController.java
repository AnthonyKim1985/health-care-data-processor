package org.bigdatacenter.dataprocessor.platform.api;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.resolver.query.common.QueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version1.MetadbVersion1Service;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version2.MetadbVersion2Service;
import org.bigdatacenter.dataprocessor.springboot.config.RabbitMQConfig;
import org.bigdatacenter.dataprocessor.springboot.exception.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final String BAD_REQUEST_MESSAGE = "invalid request (%s)";
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MetadbVersion1Service metadbVersion1Service;

    @Autowired
    @Qualifier("HiveQueryResolverVersion1Impl")
    private QueryResolver version1QueryResolver;

    @Autowired
    private MetadbVersion2Service metadbVersion2Service;

    @Autowired
    @Qualifier("HiveQueryResolverVersion2Impl")
    private QueryResolver version2QueryResolver;

    //
    // TODO: Health Care Data Extraction API
    //
    @SuppressWarnings("Duplicates")
    @RequestMapping(value = "dataExtractionVersion1", method = RequestMethod.GET)
    public void dataExtractionMetaVersion1(@RequestParam String dataSetUID, HttpServletResponse httpServletResponse) {
        if (!DataProcessorUtil.isNumeric(dataSetUID))
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "dataSetUID is not numeric."), httpServletResponse);
        logger.info(String.format("%s - Extraction data set UID: %s", currentThreadName, dataSetUID));

        if (metadbVersion1Service.isExecutedJob(Integer.parseInt(dataSetUID)))
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, String.format("dataSetUID \"%s\" has already been executed.", dataSetUID)), httpServletResponse);
        logger.info(String.format("%s - dataSetUID: %s", currentThreadName, dataSetUID));

        ExtractionParameter extractionParameter = version1QueryResolver.buildExtractionParameter(Integer.parseInt(dataSetUID));
        if (extractionParameter == null)
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution parameter map. It may be some meta data problem. Please check it out."), httpServletResponse);
        logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));

        ExtractionRequest extractionRequest = version1QueryResolver.buildExtractionRequest(extractionParameter);
        if (extractionRequest == null)
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution request object. It may be some meta data problem. Please check it out."), httpServletResponse);
        logger.info(String.format("%s - buildExtractionRequest: %s", currentThreadName, extractionRequest));

        rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, extractionRequest);
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
    }

    @SuppressWarnings("Duplicates")
    @RequestMapping(value = "dataExtractionVersion2", method = RequestMethod.GET)
    public void dataExtractionMetaVersion2(@RequestParam String dataSetUID, HttpServletResponse httpServletResponse) {
        if (!DataProcessorUtil.isNumeric(dataSetUID))
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "dataSetUID is not numeric."), httpServletResponse);
        logger.info(String.format("%s - Extraction data set UID: %s", currentThreadName, dataSetUID));

        if (metadbVersion2Service.isExecutedJob(Integer.parseInt(dataSetUID)))
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, String.format("dataSetUID \"%s\" has already been executed.", dataSetUID)), httpServletResponse);
        logger.info(String.format("%s - dataSetUID: %s", currentThreadName, dataSetUID));

        ExtractionParameter extractionParameter = version2QueryResolver.buildExtractionParameter(Integer.parseInt(dataSetUID));
        if (extractionParameter == null)
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution parameter map. It may be some meta data problem. Please check it out."), httpServletResponse);
        logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));

        ExtractionRequest extractionRequest = version2QueryResolver.buildExtractionRequest(extractionParameter);
        if (extractionRequest == null)
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution request object. It may be some meta data problem. Please check it out."), httpServletResponse);
        logger.info(String.format("%s - buildExtractionRequest: %s", currentThreadName, extractionRequest));

        rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, extractionRequest);
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
    }

    //
    // TODO: Health Care Scenario API
    //

}