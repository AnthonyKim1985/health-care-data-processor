package org.bigdatacenter.dataprocessor.platform.api;

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
import java.util.Map;

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

//    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST)
//    public void dataExtraction(@RequestBody ExtractionRequest extractionRequest, HttpServletResponse httpServletResponse) {
//        logger.info(String.format("%s - Extraction Request: %s", Thread.currentThread().getName(), extractionRequest));
//
//        rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, extractionRequest);
//        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
//    }

    @RequestMapping(value = "dataExtraction", method = RequestMethod.GET)
    public void dataExtraction(@RequestParam Integer dataSetUID, HttpServletResponse httpServletResponse) {
        logger.info(String.format("%s - Extraction data set UID: %d", Thread.currentThread().getName(), dataSetUID));

        Map<String/*db.table*/, Map<String/*column*/, List<String>/*values*/>> hiveQueryParameter = hiveQueryResolver.buildHiveQueryParameter(dataSetUID);
        logger.info(String.format("hiveQueryParameter: %s", hiveQueryParameter));

        if (hiveQueryParameter != null) {
            List<ExtractionRequest> extractionRequestList = hiveQueryResolver.buildHiveQuery(hiveQueryParameter);
            logger.info(String.format("buildHiveQuery: %s", extractionRequestList));

            if (extractionRequestList == null) {
                httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            } else {
                rabbitTemplate.convertAndSend(RabbitMQConfig.queueName, extractionRequestList);
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            }
        } else {
            httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}