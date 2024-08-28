/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.test.services;

import java.nio.file.Path;

/**
 * MockPipeLineBeanTest is a bean for holding test scenario.
 */
public class TestPipeLineBean {

    /** The name. */
    private String name;

    /** The input type. */
    private Class<? extends TestBaseContext> inputType;

    /** The output type. */
    private Class<? extends TestBaseContext> outputType;

    /** The tmp dir. */
    private Path tmpDir;

    /** The input context. */
    private TestBaseContext inputContext;

    /** The output context. */
    private TestBaseContext outputContext;

    /** The input data format. */
    private String inputDataFormat;

    /** The output data format. */
    private String outputDataFormat;

    /**
     * Instantiates a new test pipe line bean.
     *
     * @param name
     *            the name
     * @param inputType
     *            the input type
     * @param outputType
     *            the output type
     * @param inputDataFormat
     *            the input data format
     * @param outputDataFormat
     *            the output data format
     */
    public TestPipeLineBean(final String name, final Class<? extends TestBaseContext> inputType, final Class<? extends TestBaseContext> outputType,
                            final String inputDataFormat, final String outputDataFormat) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.name = name;
        this.inputDataFormat = inputDataFormat;
        this.outputDataFormat = outputDataFormat;
    }

    /**
     * Gets the tmp dir.
     *
     * @return the tmpDir
     */
    public Path getTmpDir() {
        return tmpDir;
    }

    /**
     * Sets the tmp dir.
     *
     * @param tmpDir
     *            the tmpDir to set
     */
    public void setTmpDir(final Path tmpDir) {
        this.tmpDir = tmpDir;
    }

    /**
     * Gets the input context.
     *
     * @return the inputContext
     */
    public TestBaseContext getInputContext() {
        return inputContext;
    }

    /**
     * Sets the input context.
     *
     * @param inputContext
     *            the inputContext to set
     */
    public void setInputContext(final TestBaseContext inputContext) {
        this.inputContext = inputContext;
    }

    /**
     * Gets the output context.
     *
     * @return the outputContext
     */
    public TestBaseContext getOutputContext() {
        return outputContext;
    }

    /**
     * Sets the output context.
     *
     * @param outputContext
     *            the outputContext to set
     */
    public void setOutputContext(final TestBaseContext outputContext) {
        this.outputContext = outputContext;
    }

    /**
     * Gets the input data format.
     *
     * @return the inputDataFormat
     */
    public String getInputDataFormat() {
        return inputDataFormat;
    }

    /**
     * Sets the input data format.
     *
     * @param inputDataFormat
     *            the inputDataFormat to set
     */
    public void setInputDataFormat(final String inputDataFormat) {
        this.inputDataFormat = inputDataFormat;
    }

    /**
     * Gets the output data format.
     *
     * @return the outputDataFormat
     */
    public String getOutputDataFormat() {
        return outputDataFormat;
    }

    /**
     * Sets the output data format.
     *
     * @param outputDataFormat
     *            the outputDataFormat to set
     */
    public void setOutputDataFormat(final String outputDataFormat) {
        this.outputDataFormat = outputDataFormat;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the input type.
     *
     * @return the inputType
     */
    public Class<? extends TestBaseContext> getInputType() {
        return inputType;
    }

    /**
     * Gets the output type.
     *
     * @return the outputType
     */
    public Class<? extends TestBaseContext> getOutputType() {
        return outputType;
    }
}
