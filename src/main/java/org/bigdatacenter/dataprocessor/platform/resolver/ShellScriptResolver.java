package org.bigdatacenter.dataprocessor.platform.resolver;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-17.
 */
public interface ShellScriptResolver {
    void runReducePartsMerger(String hdfsLocation);

    void runArchiveExtractedDataSet(String archiveFileName);
}
