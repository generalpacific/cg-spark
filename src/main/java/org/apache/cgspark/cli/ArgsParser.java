package org.apache.cgspark.cli;

/**
 * Interface for parsing command line arguments for CG Spark.
 */
public interface ArgsParser {
    String getInputFile();

    String getOutputFile();

    boolean isLocal();

    int getPartitionSize();
}
