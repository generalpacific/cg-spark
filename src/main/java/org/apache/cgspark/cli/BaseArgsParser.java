package org.apache.cgspark.cli;

/**
 * Abstract arguments parser which takes the arguments array.
 */
public abstract class BaseArgsParser implements ArgsParser {
    protected final String[] args;

    public BaseArgsParser(final String[] args) {
        this.args = args;
    }
}
