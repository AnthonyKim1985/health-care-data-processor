package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequestVersion2 extractionRequest);
}
