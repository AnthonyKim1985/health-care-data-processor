package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.version1.ExtractionRequestVersion1;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequestVersion1 extractionRequest);
}
