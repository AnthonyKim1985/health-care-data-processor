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
        if (extractionRequestList == null) {
            logger.error(String.format("%s - Error occurs : extraction request list is null", Thread.currentThread().getName()));
            return;
        }

        final int size = extractionRequestList.size();
        for (int i = 0; i < size; i++) {
            ExtractionRequest extractionRequest = extractionRequestList.get(i);
            logger.info(String.format("%s - Remaining %d query processing", Thread.currentThread().getName(), (size - i)));
            logger.info(String.format("%s - Start data extraction at Hive Query: %s", Thread.currentThread().getName(), extractionRequest));

            long beginTime = System.currentTimeMillis();
            hiveService.extractDataByHiveQL(extractionRequest);

            logger.info(String.format("%s - Finish data extraction at Hive Query: %s, Elapsed time: %d ms",
                    Thread.currentThread().getName(), extractionRequest, (System.currentTimeMillis() - beginTime)));
        }
    }
}