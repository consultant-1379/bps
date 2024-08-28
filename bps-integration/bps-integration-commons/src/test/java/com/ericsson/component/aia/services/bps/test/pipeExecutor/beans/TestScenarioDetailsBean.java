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
package com.ericsson.component.aia.services.bps.test.pipeExecutor.beans;

import java.util.LinkedHashSet;
import java.util.Set;

public class TestScenarioDetailsBean {

    private Set<TestAdapterBean> inputAdapters = new LinkedHashSet<>();
    private Set<TestAdapterBean> outputAdapters = new LinkedHashSet<>();
    private String testScenario;

    /**
     * @return the testScenario
     */
    public String getTestScenario() {
        return testScenario;
    }

    /**
     * @param testScenario
     *            the testScenario to set
     */
    public void setTestScenario(final String testScenario) {
        this.testScenario = testScenario;
    }

    @Override
    public String toString() {
        return "TestScenarioDetailsBean [inputAdapters=" + inputAdapters + ", outputAdapters=" + outputAdapters + "]";
    }

    /**
     * @return the inputAdapters
     */
    public Set<TestAdapterBean> getInputAdapters() {
        return inputAdapters;
    }

    /**
     * @param inputAdapters
     *            the inputAdapters to set
     */
    public void setInputAdapters(final Set<TestAdapterBean> inputAdapters) {
        this.inputAdapters = inputAdapters;
    }

    /**
     * @return the outputAdapters
     */
    public Set<TestAdapterBean> getOutputAdapters() {
        return outputAdapters;
    }

    /**
     * @param outputAdapters
     *            the outputAdapters to set
     */
    public void setOutputAdapters(final Set<TestAdapterBean> outputAdapters) {
        this.outputAdapters = outputAdapters;
    }

}
