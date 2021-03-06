package org.bigdatacenter.dataprocessor.platform.resolver.script;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-17.
 */
@Component
public final class ShellScriptResolverImpl implements ShellScriptResolver {
    private static final Logger logger = LoggerFactory.getLogger(ShellScriptResolverImpl.class);

    @Override
    public void runReducePartsMerger(String hdfsLocation, String header) {
        fork(CommandBuilder.buildReducePartsMerger(hdfsLocation, header));
    }

    @Override
    public void runArchiveExtractedDataSet(String archiveFileName, String ftpLocation) {
        fork(CommandBuilder.buildArchiveExtractedDataSet(archiveFileName, ftpLocation));
    }

    private void fork(String target) {
        try {
            Process process = Runtime.getRuntime().exec(target);

            final Thread stdinStreamResolver = new Thread(new InputStreamResolver("input_stream", process.getInputStream()));
            stdinStreamResolver.start();

            final Thread stderrStreamResolver = new Thread(new InputStreamResolver("error_stream", process.getErrorStream()));
            stderrStreamResolver.start();

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            logger.warn(String.format("%s - Forked process occurs an exception: %s", Thread.currentThread().getName(), e.getMessage()));
        }
    }

    private static final class CommandBuilder implements Serializable {
        static String buildReducePartsMerger(String hdfsLocation, String header) {
            return String.format("sh sh/hdfs-parts-merger.sh %s %s", hdfsLocation, header);
        }

        static String buildArchiveExtractedDataSet(String archiveFileName, String ftpLocation) {
            return String.format("sh sh/archive-data-set.sh %s %s", archiveFileName, ftpLocation);
        }
    }

    @Data
    @AllArgsConstructor
    private final class InputStreamResolver implements Runnable, Serializable {
        private final String streamName;
        private final InputStream inputStream;

        @Override
        public void run() {
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

            try (FileWriter fileWriter = new FileWriter(new File(String.format("logs/sh/%s_%s.log", simpleDateFormat.format(new Date()), streamName)), true);
                 BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    fileWriter.write(String.format("[%s] %s\n", new Date().toString(), line));
                    fileWriter.flush();
                }
            } catch (IOException e) {
                logger.warn(String.format("%s - Forked process occurs an exception: %s", Thread.currentThread().getName(), e.getMessage()));
            }
        }
    }
}