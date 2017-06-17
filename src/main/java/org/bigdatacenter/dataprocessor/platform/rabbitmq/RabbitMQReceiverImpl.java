package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.resolver.ShellScriptResolver;
import org.bigdatacenter.dataprocessor.platform.service.hive.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);

    @Autowired
    private HiveService hiveService;

    @Autowired
    private ShellScriptResolver shellScriptResolver;

    @Override
    public void runReceiver(List<ExtractionRequest> extractionRequestList) {
        if (extractionRequestList == null) {
            logger.error(String.format("%s - Error occurs : extraction request list is null", Thread.currentThread().getName()));
            return;
        }

        long jobBeginTime = System.currentTimeMillis();
        final int size = extractionRequestList.size();
        for (int i = 0; i < size; i++) {
            ExtractionRequest extractionRequest = extractionRequestList.get(i);
            logger.info(String.format("%s - Remaining %d query processing", Thread.currentThread().getName(), (size - i)));
            logger.info(String.format("%s - Start data extraction at Hive Query: %s", Thread.currentThread().getName(), extractionRequest));

            long queryBeginTime = System.currentTimeMillis();
            hiveService.extractDataByHiveQL(extractionRequest);

            //
            // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
            //
            String hdfsLocation = extractionRequest.getHdfsLocation();
            shellScriptResolver.runReducePartsMerger(hdfsLocation);

            logger.info(String.format("%s - Finish data extraction at Hive Query: %s, Elapsed time: %d ms",
                    Thread.currentThread().getName(), extractionRequest, (System.currentTimeMillis() - queryBeginTime)));

            //
            // TODO: 쿼리 상태를 Meta-DB 에 갱신한다.
            //
        }

        //
        // TODO: Archive the extracted data set and finally send the file to FTP server.
        //
        shellScriptResolver.runArchiveExtractedDataSet(String.format("archive_%s", new Timestamp(System.currentTimeMillis()).getTime()));

        logger.info(String.format("%s - All job is done, Elapsed time: %d ms",
                Thread.currentThread().getName(), (System.currentTimeMillis() - jobBeginTime)));
    }
}