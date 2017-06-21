package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.RequestInfo;
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
    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private HiveService hiveService;

    @Autowired
    private ShellScriptResolver shellScriptResolver;


    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {
        if (!validateRequest(extractionRequest)) {
            logger.error(String.format("%s - Error occurs : extraction request list is null", currentThreadName));
            return;
        }

        final RequestInfo requestInfo = extractionRequest.getRequestInfo();
        final List<HiveTask> hiveTaskList = extractionRequest.getHiveTaskList();

        final long jobBeginTime = System.currentTimeMillis();

        logger.info(String.format("%s - Start RabbitMQ Message Receiver task", currentThreadName));

        runQueryTask(hiveTaskList);
        runArchiveTask(requestInfo);

        logger.info(String.format("%s - All job is done, Elapsed time: %d ms", currentThreadName, (System.currentTimeMillis() - jobBeginTime)));
    }

    private boolean validateRequest(ExtractionRequest extractionRequest) {
        if (extractionRequest == null)
            return false;
        else if (extractionRequest.getHiveTaskList().size() == 0)
            return false;

        return true;
    }

    private void runQueryTask(List<HiveTask> hiveTaskList) {
        final int MaxHiveTasks = hiveTaskList.size();
        for (int i = 0; i < MaxHiveTasks; i++) {
            HiveTask hiveTask = hiveTaskList.get(i);
            logger.info(String.format("%s - Remaining %d query processing", currentThreadName, (MaxHiveTasks - i)));
            logger.info(String.format("%s - Start data extraction at Hive Query: %s", currentThreadName, hiveTask));

            final long queryBeginTime = System.currentTimeMillis();
            hiveService.extractDataByHiveQL(hiveTask);

            //
            // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
            //
            final String hdfsLocation = hiveTask.getHdfsLocation();
            shellScriptResolver.runReducePartsMerger(hdfsLocation);

            //
            // TODO: Update meta database
            //


            logger.info(String.format("%s - Finish data extraction at Hive Query: %s, Elapsed time: %d ms", currentThreadName, hiveTask, (System.currentTimeMillis() - queryBeginTime)));
        }
    }

    private void runArchiveTask(RequestInfo requestInfo) {
        //
        // TODO: Archive the extracted data set and finally send the file to FTP server.
        //
        final String archiveFileName = String.format("archive_%s_%s", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
        final String ftpLocation = String.format("/%s/%s/%s", String.valueOf(requestInfo.getGroupUID()), requestInfo.getUserID(), archiveFileName);

        final long archiveFileBeginTime = System.currentTimeMillis();
        logger.info(String.format("%s - Start archiving the extracted data set: %s", currentThreadName, archiveFileName));
        shellScriptResolver.runArchiveExtractedDataSet(archiveFileName, ftpLocation);
        logger.info(String.format("%s - Finish archiving the extracted data set: %s, Elapsed time: %d ms", currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

        //
        // TODO: Update meta database
        //

    }
}