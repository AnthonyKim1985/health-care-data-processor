package org.bigdatacenter.dataprocessor.platform.api;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.bigdatacenter.dataprocessor.platform.resolver.query.HiveQueryResolver;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.bigdatacenter.dataprocessor.springboot.config.RabbitMQConfig;
import org.bigdatacenter.dataprocessor.springboot.exception.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
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
    private MetadbService metadbService;

    @Autowired
    private HiveQueryResolver hiveQueryResolver;

    //
    // TODO: Health Care Data Extraction API
    //
    @RequestMapping(value = "dataExtraction", method = RequestMethod.GET)
    public void dataExtraction(@RequestParam String dataSetUID, HttpServletResponse httpServletResponse) {
        runDataExtraction(dataSetUID, httpServletResponse);
    }

    private void runDataExtraction(String dataSetUID, HttpServletResponse httpServletResponse) {
        if (!DataProcessorUtil.isNumeric(dataSetUID)) {
            throw new RestException(String.format(BAD_REQUEST_MESSAGE, "dataSetUID is not numeric."), httpServletResponse);
        }
        logger.info(String.format("%s - Extraction data set UID: %s", currentThreadName, dataSetUID));

        if (metadbService.isExecutedJob(Integer.parseInt(dataSetUID))) {
            metadbService.updateProcessState(Integer.parseInt(dataSetUID), MetadbMapper.PROCESS_STATE_REJECTED);
            throw new RestException(Integer.parseInt(dataSetUID), String.format(BAD_REQUEST_MESSAGE, String.format("dataSetUID \"%s\" has already been executed.", dataSetUID)), httpServletResponse);
        }
        logger.info(String.format("%s - dataSetUID: %s", currentThreadName, dataSetUID));

        ExtractionParameter extractionParameter = hiveQueryResolver.buildExtractionParameter(Integer.parseInt(dataSetUID));
        if (extractionParameter == null) {
            metadbService.updateProcessState(Integer.parseInt(dataSetUID), MetadbMapper.PROCESS_STATE_REJECTED);
            throw new RestException(Integer.parseInt(dataSetUID), String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution parameter map. It may be some meta data problem. Please check it out."), httpServletResponse);
        }
        logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));

        ExtractionRequest extractionRequest = hiveQueryResolver.buildExtractionRequest(extractionParameter);
        if (extractionRequest == null) {
            metadbService.updateProcessState(Integer.parseInt(dataSetUID), MetadbMapper.PROCESS_STATE_REJECTED);
            throw new RestException(Integer.parseInt(dataSetUID), String.format(BAD_REQUEST_MESSAGE, "Couldn't make the execution request object. It may be some meta data problem. Please check it out."), httpServletResponse);
        }
        logger.info(String.format("%s - buildExtractionRequest: %s", currentThreadName, extractionRequest));

        synchronized (this) {
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, extractionRequest);
        }
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
    }

    //
    // TODO: Health Care Data Work-flow API
    //
    @RequestMapping(value = "dataWorkFlow", method = RequestMethod.GET)
    public void dataWorkFlow(HttpServletResponse httpServletResponse) throws InterruptedException {
        logger.info(String.format("%s - dataWorkFlow started.", currentThreadName));
        synchronized (this) {
            Thread.sleep(10000L);
        }
        logger.info(String.format("%s - dataWorkFlow finished.", currentThreadName));
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
    }
}