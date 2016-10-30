package org.apache.cgspark.cli;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CommonsCliArgsParserTest {

    private static final String TEST_INPUT = "Testinput";
    private static final String TEST_OUTPUT = "Testouptut";
    private static final int TEST_PARTITION = 1232;

    @Test
    public void testValidArgsShort() throws ParseException {
        final String[] testValidArgsShort = {"-i", TEST_INPUT, "-o",
                TEST_OUTPUT, "-l", "-p", Integer.toString(TEST_PARTITION)};

        CommonsCliArgsParser parser = new CommonsCliArgsParser
                (testValidArgsShort, "testValidArgsShort");
        assertThat("Input arg doesnt match", parser.getInputFile(), is
                (TEST_INPUT));
        assertThat("Output arg doesnt match", parser.getOutputFile(), is
                (TEST_OUTPUT));
        assertThat("isLocal arg doesnt match", parser.isLocal(), is(true));
        assertThat("partition arg doesnt match", parser.getPartitionSize(),
                is(TEST_PARTITION));
    }

    @Test
    public void testValidArgsLong() throws ParseException {
        final String[] testValidArgsLong = {"-input", TEST_INPUT, "-output",
                TEST_OUTPUT, "-isLocal", "-partition", Integer.toString
                (TEST_PARTITION)};

        CommonsCliArgsParser parser = new CommonsCliArgsParser
                (testValidArgsLong, "testValidArgsLong");
        assertThat("Input arg doesnt match", parser.getInputFile(), is
                (TEST_INPUT));
        assertThat("Output arg doesnt match", parser.getOutputFile(), is
                (TEST_OUTPUT));
        assertThat("isLocal arg doesnt match", parser.isLocal(), is(true));
        assertThat("partition arg doesnt match", parser.getPartitionSize(),
                is(TEST_PARTITION));
    }

    @Test
    public void testValidArgsShortWOLocal() throws ParseException {
        final String[] testValidArgsShort = {"-i", TEST_INPUT, "-o",
                TEST_OUTPUT, "-p", Integer.toString(TEST_PARTITION)};

        CommonsCliArgsParser parser = new CommonsCliArgsParser
                (testValidArgsShort, "testValidArgsShortWOLocal");
        assertThat("Input arg doesnt match", parser.getInputFile(), is
                (TEST_INPUT));
        assertThat("Output arg doesnt match", parser.getOutputFile(), is
                (TEST_OUTPUT));
        assertThat("isLocal arg doesnt match", parser.isLocal(), is(false));
        assertThat("partition arg doesnt match", parser.getPartitionSize(),
                is(TEST_PARTITION));
    }

    @Test
    public void testValidArgsLongWOLocal() throws ParseException {
        final String[] testValidArgsLong = {"-input", TEST_INPUT, "-output",
                TEST_OUTPUT, "-partition", Integer.toString(TEST_PARTITION)};

        CommonsCliArgsParser parser = new CommonsCliArgsParser
                (testValidArgsLong, "testValidArgsLongWOLocal");
        assertThat("Input arg doesnt match", parser.getInputFile(), is
                (TEST_INPUT));
        assertThat("Output arg doesnt match", parser.getOutputFile(), is
                (TEST_OUTPUT));
        assertThat("isLocal arg doesnt match", parser.isLocal(), is(false));
        assertThat("partition arg doesnt match", parser.getPartitionSize(),
                is(TEST_PARTITION));
    }

    @Test(expected = ParseException.class)
    public void testInvalidArgsLongInvalidOpt() throws ParseException {
        final String[] testInvalidOpt = {"-xyz", TEST_INPUT, "-output",
                TEST_OUTPUT, "-isLocal", "-partition", Integer.toString
                (TEST_PARTITION)};

        new CommonsCliArgsParser(testInvalidOpt, "testInvalidArgsLongInvalidOpt");
    }

    @Test(expected = ParseException.class)
    public void testInvalidArgsLongNoValueOpt() throws ParseException {
        final String[] testInvalidOpt = {"-input", TEST_INPUT, "-output",
                "-isLocal", "-partition", Integer.toString(TEST_PARTITION)};

        new CommonsCliArgsParser(testInvalidOpt, "testInvalidArgsLongNoValueOpt");
    }

    @Test(expected = ParseException.class)
    public void testInvalidArgsNonIntegerPartitionSize() throws ParseException {
        final String[] testValidArgsLong = {"-input", TEST_INPUT, "-output",
                TEST_OUTPUT, "-isLocal", "-partition", "NotAnInteger"};

        new CommonsCliArgsParser(testValidArgsLong,
                "testInvalidArgsNonIntegerPartitionSize");
    }
}
