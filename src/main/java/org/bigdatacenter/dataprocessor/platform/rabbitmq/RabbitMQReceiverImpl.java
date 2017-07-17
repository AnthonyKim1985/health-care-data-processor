package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.extraction.ExtractionRequest;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.creation.HiveCreationTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.task.extraction.HiveExtractionTask;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.MetadbMapper;
import org.bigdatacenter.dataprocessor.platform.resolver.script.ShellScriptResolver;
import org.bigdatacenter.dataprocessor.platform.service.hive.HiveService;
import org.bigdatacenter.dataprocessor.platform.service.metadb.MetadbService;
import org.bigdatacenter.dataprocessor.springboot.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-02.
 */
@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String currentThreadName = Thread.currentThread().getName();

    @Autowired
    private HiveService hiveService;

    @Autowired
    private MetadbService metadbService;

    @Autowired
    private ShellScriptResolver shellScriptResolver;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {
        final RequestInfo requestInfo = extractionRequest.getRequestInfo();
        final int dataSetUID = requestInfo.getDataSetUID();

        if (validateRequest(extractionRequest)) {
            try {
                final long jobBeginTime = System.currentTimeMillis();

                logger.info(String.format("%s - Start RabbitMQ Message Receiver task", currentThreadName));
                metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_PROCESSING);
                metadbService.updateJobStartTime(dataSetUID, dateFormat.format(new Date(jobBeginTime)));

                runQueryTask(extractionRequest);
                runArchiveTask(requestInfo);

                final long jobEndTime = System.currentTimeMillis();
                final long elapsedTime = jobEndTime - jobBeginTime;

                logger.info(String.format("%s - All job is done, Elapsed time: %d ms", currentThreadName, elapsedTime));
                metadbService.updateJobEndTime(dataSetUID, dateFormat.format(new Date(jobEndTime)));
                metadbService.updateElapsedTime(dataSetUID, getElapsedTime(elapsedTime));
                metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_COMPLETED);
            } catch (Exception e) {
                rabbitAdmin.purgeQueue(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, true);
                metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
                logger.error(String.format("%s - Exception occurs in RabbitMQReceiver : %s", currentThreadName, e.getMessage()));
                logger.error(String.format("%s - The extraction request has been purged in queue. (%s)", currentThreadName, extractionRequest));
            }
        } else {
            metadbService.updateProcessState(dataSetUID, MetadbMapper.PROCESS_STATE_REJECTED);
            logger.error(String.format("%s - Drop the job. (%s)", currentThreadName, extractionRequest));
        }
    }

    private String getElapsedTime(long millis) {
        return String.format("%02d:%02d:%02d",
                TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));
    }

    private boolean validateRequest(ExtractionRequest extractionRequest) {
        if (extractionRequest == null) {
            logger.error(String.format("%s - Error occurs at RabbitMQReceiver: extraction request is null", currentThreadName));
            return false;
        } else if (extractionRequest.getHiveTaskList().size() == 0) {
            logger.error(String.format("%s - Error occurs at RabbitMQReceiver: There are no Hive tasks to process.", currentThreadName));
            return false;
        }
        return true;
    }

    private void runQueryTask(ExtractionRequest extractionRequest) {
        final List<HiveTask> hiveTaskList = extractionRequest.getHiveTaskList();
        final int MAX_TASKS = hiveTaskList.size();

        for (int i = 0; i < MAX_TASKS; i++) {
            final HiveTask hiveTask = hiveTaskList.get(i);
            final HiveCreationTask hiveCreationTask = hiveTask.getHiveCreationTask();
            final HiveExtractionTask hiveExtractionTask = hiveTask.getHiveExtractionTask();

            final long queryBeginTime = System.currentTimeMillis();
            logger.info(String.format("%s - Remaining %d query processing", currentThreadName, (MAX_TASKS - i)));

            logger.info(String.format("%s - Start table creation at Hive Query: %s", currentThreadName, hiveCreationTask.getHiveQuery()));
            hiveService.createTableByHiveQL(hiveCreationTask);

            if (hiveExtractionTask != null) {
                logger.info(String.format("%s - Start data extraction at Hive Query: %s", currentThreadName, hiveExtractionTask.getHiveQuery()));
                hiveService.extractDataByHiveQL(hiveExtractionTask);

                //
                // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
                //
                final String hdfsLocation = hiveExtractionTask.getHdfsLocation();
                final String header = hiveExtractionTask.getHeader();
                shellScriptResolver.runReducePartsMerger(hdfsLocation, header);
            }

            //
            // TODO: Update transaction database
            //

            final long queryEndTime = System.currentTimeMillis() - queryBeginTime;
            logger.info(String.format("%s - Finish Hive Query: %s, Elapsed time: %d ms", currentThreadName, hiveTask, queryEndTime));
        }
    }

    private void runArchiveTask(RequestInfo requestInfo) {
        //
        // TODO: Archive the extracted data set and finally send the file to FTP server.
        //
        final String archiveFileName = String.format("%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
        final String ftpLocation = String.format("/%s/%s", requestInfo.getUserID(), requestInfo.getDatasetName());

        final long archiveFileBeginTime = System.currentTimeMillis();
        logger.info(String.format("%s - Start archiving the extracted data set: %s", currentThreadName, archiveFileName));
        shellScriptResolver.runArchiveExtractedDataSet(archiveFileName, ftpLocation);
        logger.info(String.format("%s - Finish archiving the extracted data set: %s, Elapsed time: %d ms", currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

        //
        // TODO: Update meta database
        //

        final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
        metadbService.insertFtpRequest(new FtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI));
    }
}