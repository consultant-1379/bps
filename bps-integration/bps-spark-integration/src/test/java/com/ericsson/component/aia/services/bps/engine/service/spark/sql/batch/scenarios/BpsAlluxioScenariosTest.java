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
package com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.scenarios;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.ericsson.component.aia.services.bps.test.enums.TestType;
import com.ericsson.component.aia.services.bps.test.pipeExecutor.beans.TestScenarioDetailsBean;

@RunWith(Parameterized.class)
public class BpsAlluxioScenariosTest extends BpsSparkBaseCommon {

    /**
     * Instantiates a new test BPS pipe line executer jdbc cases.
     *
     */
    public BpsAlluxioScenariosTest(final TestScenarioDetailsBean testScenarioBean) {
        super(testScenarioBean);
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @return the collection
     * @throws Exception
     *             the exception
     */
    @Parameters(name = "{index}: Validating Scenario [ {0} ]")
    public static List<Object> data() throws Exception {
        return provideData(TestType.ALLUXIO);
    }

    /**
     * Executes all test cases related to File.
     *
     * @throws Exception
     *             the exception
     */
    @Test
    @Ignore
    public void testAlluxioScenarios() throws Exception {
        setProperties(ScenarioType.ALLUXIO);
        testScenario();
    }
}
