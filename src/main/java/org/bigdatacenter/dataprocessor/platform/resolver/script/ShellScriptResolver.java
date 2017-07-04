package org.bigdatacenter.dataprocessor.platform.resolver.script;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-17.
 */
public interface ShellScriptResolver {
    void runReducePartsMerger(String hdfsLocation, String header);

    void runArchiveExtractedDataSet(String archiveFileName, String ftpLocation);
}
