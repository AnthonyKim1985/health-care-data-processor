package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.service.hive.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);

    @Autowired
    private HiveService hiveService;

    @Override
    public void runReceiver(List<ExtractionRequest> extractionRequestList) {
        logger.info(String.format("%s - extractionRequestList: %s",
                Thread.currentThread().getName(), extractionRequestList));

        for (ExtractionRequest extractionRequest : extractionRequestList) {
            long beginTime = System.currentTimeMillis();
            hiveService.extractDataByHiveQL(extractionRequest);
            logger.info(String.format("%s - Finish Hive Query in dataExtraction: %s, Elapsed time: %d ms",
                    Thread.currentThread().getName(), extractionRequest, (System.currentTimeMillis() - beginTime)));
        }
    }
}