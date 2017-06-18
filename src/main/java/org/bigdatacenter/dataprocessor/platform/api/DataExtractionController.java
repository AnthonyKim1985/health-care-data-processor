package org.bigdatacenter.dataprocessor.platform.api;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionParameter;
import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.resolver.HiveQueryResolver;
import org.bigdatacenter.dataprocessor.springboot.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-05-30.
 */
@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private HiveQueryResolver hiveQueryResolver;

    @RequestMapping(value = "dataExtraction", method = RequestMethod.GET)
    public void dataExtraction(@RequestParam Integer dataSetUID, HttpServletResponse httpServletResponse) {
        logger.info(String.format("%s - Extraction data set UID: %d", Thread.currentThread().getName(), dataSetUID));

        ExtractionParameter extractionParameter = hiveQueryResolver.buildHiveQueryParameter(dataSetUID);
        logger.info(String.format("extractionParameter: %s", extractionParameter));

        if (extractionParameter != null) {
            List<ExtractionRequest> extractionRequestList = hiveQueryResolver.buildHiveQuery(extractionParameter);
            logger.info(String.format("buildHiveQuery: %s", extractionRequestList));

            if (extractionRequestList != null) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, extractionRequestList);
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            } else {
                httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
        } else {
            httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}