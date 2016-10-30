package org.apache.cgspark.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

/**
 * Implementation of Command Line Arguments parser based on Commons CLI Library.
 */
public class CommonsCliArgsParser extends BaseArgsParser {

    private final String inputFile;
    private final String outputFile;
    private final boolean isLocal;
    private final int partitionSize;

    private final static Options OPTIONS = new Options();
    private final static String INPUT_OPT = "input";
    private final static String OUTPUT_OPT = "output";
    private final static String IS_LOCAL_OPT = "isLocal";
    private final static String PARTITION_OPT = "partition";
    private final static CommandLineParser PARSER = new GnuParser();
    private final static HelpFormatter HELP_FORMATTER = new HelpFormatter();

    static {
        final Option inputFileOption = new Option("i", INPUT_OPT, true,
                "Input File");
        inputFileOption.setRequired(true);
        OPTIONS.addOption(inputFileOption);

        final Option outputFileOption = new Option("o", OUTPUT_OPT, true,
                "Output File");
        outputFileOption.setRequired(true);
        OPTIONS.addOption(outputFileOption);

        final Option isLocalOption = new Option("l", IS_LOCAL_OPT, false,
                "Run locally instead of as spark job");
        OPTIONS.addOption(isLocalOption);

        final Option partionSizeOption = new Option("p", PARTITION_OPT, true,
                "Partition Size");
        partionSizeOption.setRequired(true);
        OPTIONS.addOption(partionSizeOption);
    }

    public CommonsCliArgsParser(String[] args, String operation) throws
            ParseException {
        super(args);
        final CommandLine commandLine;
        try {
            commandLine = PARSER.parse(OPTIONS, args);
        } catch (ParseException e) {
            HELP_FORMATTER.printHelp(operation, OPTIONS);
            throw e;
        }
        inputFile = getStringValue(commandLine, INPUT_OPT);
        outputFile = getStringValue(commandLine, OUTPUT_OPT);
        isLocal = commandLine.hasOption(IS_LOCAL_OPT);
        partitionSize = getIntValue(commandLine, PARTITION_OPT);
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

    private static String getStringValue(CommandLine commandLine, String opt)
            throws ParseException {
        final String value = commandLine.getOptionValue(opt);
        if (StringUtils.isEmpty(value)) {
            throw new ParseException(opt + " value not present");
        }
        return value;
    }

    private static int getIntValue(CommandLine commandLine, String opt)
            throws ParseException {
        final String optionValue = commandLine.getOptionValue(opt);
        try {
            return Integer.parseInt(optionValue);
        } catch (NullPointerException | NumberFormatException ex) {
            throw new ParseException("Invalid " + opt + " value: " +
                    optionValue);
        }
    }
}
