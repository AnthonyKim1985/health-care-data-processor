package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;

import java.util.List;

/**
 * Created by hyuk0 on 2017-06-02.
 */
public interface RabbitMQReceiver {
    void runReceiver(List<ExtractionRequest> extractionRequestList);
}
