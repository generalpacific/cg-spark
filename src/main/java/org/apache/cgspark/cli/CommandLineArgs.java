package org.apache.cgspark.cli;

/**
 * Arguments read from command line.
 */
public class CommandLineArgs {
    private final String inputFile;
    private final String outputFile;
    private final boolean isLocal;
    private final int partitionSize;

    public CommandLineArgs(final String inputFile, final String outputFile,
                           final boolean isLocal, final int partitionSize) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.isLocal = isLocal;
        this.partitionSize = partitionSize;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public int getPartitionSize() {
        return partitionSize;
    }
}
