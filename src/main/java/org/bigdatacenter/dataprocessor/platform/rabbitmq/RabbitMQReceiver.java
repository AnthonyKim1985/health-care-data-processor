package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.request.ExtractionRequest;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequest extractionRequest);
}
