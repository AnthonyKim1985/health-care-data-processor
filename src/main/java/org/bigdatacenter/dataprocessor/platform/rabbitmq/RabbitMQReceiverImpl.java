package org.bigdatacenter.dataprocessor.platform.rabbitmq;

import org.bigdatacenter.dataprocessor.platform.domain.hive.common.HiveTask;
import org.bigdatacenter.dataprocessor.platform.domain.hive.version2.ExtractionRequestVersion2;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.common.FtpInfo;
import org.bigdatacenter.dataprocessor.platform.domain.metadb.version2.request.RequestInfo;
import org.bigdatacenter.dataprocessor.platform.persistence.metadb.version2.MetadbVersion2Mapper;
import org.bigdatacenter.dataprocessor.platform.resolver.script.ShellScriptResolver;
import org.bigdatacenter.dataprocessor.platform.service.hive.HiveService;
import org.bigdatacenter.dataprocessor.platform.service.metadb.version2.MetadbVersion2Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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
    private MetadbVersion2Service metadbService;

    @Autowired
    private ShellScriptResolver shellScriptResolver;

    @Override
    public void runReceiver(ExtractionRequestVersion2 extractionRequest) {
        if (!validateRequest(extractionRequest)) {
            logger.error(String.format("%s - Error occurs : extraction request list is null", currentThreadName));
            return;
        }

        final RequestInfo requestInfo = extractionRequest.getRequestInfo();
        final List<HiveTask> hiveTaskList = extractionRequest.getHiveTaskList();
        final long jobBeginTime = System.currentTimeMillis();

        logger.info(String.format("%s - Start RabbitMQ Message Receiver task", currentThreadName));

        metadbService.updateProcessState(requestInfo.getDataSetUID(), MetadbVersion2Mapper.PROCESS_STATE_PROCESSING);
        metadbService.updateJobStartTime(requestInfo.getDataSetUID(), dateFormat.format(new Date(jobBeginTime)));

        runQueryTask(hiveTaskList);
        runArchiveTask(requestInfo);

        final long jobEndTime = System.currentTimeMillis();
        final long elapsedTime = jobEndTime - jobBeginTime;

        logger.info(String.format("%s - All job is done, Elapsed time: %d ms", currentThreadName, elapsedTime));

        metadbService.updateJobEndTime(requestInfo.getDataSetUID(), dateFormat.format(new Date(jobEndTime)));
        metadbService.updateElapsedTime(requestInfo.getDataSetUID(), dateFormat.format(new Date(elapsedTime)));
        metadbService.updateProcessState(requestInfo.getDataSetUID(), MetadbVersion2Mapper.PROCESS_STATE_COMPLETED);
    }

    private boolean validateRequest(ExtractionRequestVersion2 extractionRequest) {
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
            // TODO: Update transaction database
            //


            logger.info(String.format("%s - Finish data extraction at Hive Query: %s, Elapsed time: %d ms", currentThreadName, hiveTask, (System.currentTimeMillis() - queryBeginTime)));
        }
    }

    private void runArchiveTask(RequestInfo requestInfo) {
        //
        // TODO: Archive the extracted data set and finally send the file to FTP server.
        //
        final String archiveFileName = String.format("archive_%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
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