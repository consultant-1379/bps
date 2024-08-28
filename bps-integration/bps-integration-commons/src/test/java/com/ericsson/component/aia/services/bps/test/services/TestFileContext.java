package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.INPUT_DATA_FOLDER;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.test.enums.TestType;

/**
 * TestFileContext is one of the implementation for BaseMockContext and it is useful in creating and validating FILE related test cases.
 */
public class TestFileContext extends TestBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestFileContext.class);

    /** The output folder. */
    private String outputFolder;

    /**
     * Instantiates a new mock file context.
     *
     * @param testScenario
     *            the test scenario
     */
    public TestFileContext(final String testScenario) {
        super(testScenario);
        this.testScenario = testScenario;
    }

    /**
     * Initialize input configs.
     */
    public void initializeInputConfigs() {
        final String INPUT_FILE = getFileUri() + INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(inputDataFormat);
        initializeInputConfigs(TestType.FILE.name(), INPUT_FILE);
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> getInputConfigurations() {

        if (inputConfiguration == null) {
            initializeInputConfigs();
        }
        return inputConfiguration;
    };

    /**
     * Initialize output configs.
     */
    private void initializeOutputConfigs() {
        outputFolder = tmpDir.toAbsolutePath().toString() + SEPARATOR + "output-folder" + SEPARATOR + outputDataFormat;
        initializeOutputConfigs(TestType.FILE.name(), getFileUri() + outputFolder);
    }

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> getOutputConfigurations() {

        if (outputConfiguration == null) {
            initializeOutputConfigs();
        }
        return outputConfiguration;
    };

    /**
     * StepConfigurations for a Job as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> getStepConfigurations() {
        final Map<String, String> stepMap = super.getStepConfigurations();
        stepMap.put("master.url", "local[*]");
        return stepMap;
    }

    /**
     * Validates expected and actual output data.
     */
    @Override
    public void validate() {
        try {
            validateFileOutput(outputFolder, EXPECTED_OUTPUT_DIR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
        } catch (final Exception e) {
            LOGGER.info("validate operation failed got Exception", e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {
        if (cleanUp) {
            try {
                FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
            } catch (final IOException e) {
                LOGGER.debug("CleanUp, IOException", e);
            }
        }
    }
}
