package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.HIVE_URL;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.WORKING_DIR;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hive.jdbc.HiveDriver;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.test.cluster.util.SshTerminalClientUtil;
import com.ericsson.component.aia.services.bps.test.common.util.CreateTableInDatabase;
import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.ericsson.component.aia.services.bps.test.mock.embedded.services.MockHiveTestService;

/**
 * TestHiveContext helps in creating/connecting to Hive server. It also validates Expected vs Actual outputs.
 */
public class TestHiveContext extends TestBaseContext {

    /** The Constant LOGGER. */
    private MockHiveTestService hiveTestService;

    /** The table ip name. */
    private String tableIpName;

    /** The table op name. */
    private String tableOpName;

    /** The warehouse dir. */
    private String warehouseDir;

    /** The input file. */
    private String filePath;

    private SshTerminalClientUtil sshTerminalClientUtil;

    /**
     * Instantiates a new test mock hive context.
     *
     * @param testScenario
     *            the test scenario
     * @throws Exception
     *             the exception
     */
    public TestHiveContext(final String testScenario) throws Exception {
        super(testScenario);
        this.testScenario = testScenario;
        if (executionMode == ExecutionMode.EMBEDDED) {
            hiveTestService = MockHiveTestService.getInstance(testScenario);
        } else {
            sshTerminalClientUtil = new SshTerminalClientUtil();
            sshTerminalClientUtil.setProperties(properties);
        }
    }

    /**
     * Input configurations.
     *
     * @return the map
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @Override
    public Map<String, String> getInputConfigurations() throws IOException {
        if (inputConfiguration == null) {
            initializeInputConfigs();
        }
        return inputConfiguration;
    }

    /**
     * Output configurations.
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
     * Validate.
     *
     * @throws Exception
     *             the exception
     */
    @Override
    public void validate() throws Exception {
        final String outputDirectory;
        if (executionMode == ExecutionMode.CLUSTER) {
            final File localFiles = copyHiveOutputFile(warehouseDir);
            outputDirectory = localFiles.getAbsoluteFile().getPath() + SEPARATOR + tableOpName;
        } else {
            outputDirectory = warehouseDir + SEPARATOR + tableOpName;
        }

        validateFileOutput(outputDirectory, EXPECTED_OUTPUT_DIR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
    }

    private File copyHiveOutputFile(final String warehouseDir) throws InterruptedException {
        File localCopiedFiles = null;
        try {
            final Path path = new Path(warehouseDir);
            sshTerminalClientUtil.copyFilesFromFSToCluster(path, testScenario + SEPARATOR, ScenarioType.HDFS);
            Thread.sleep(3000);
            localCopiedFiles = sshTerminalClientUtil.copyFilesFromClusterToLocal(testScenario + SEPARATOR + "output" + SEPARATOR, ScenarioType.HDFS);
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }
        return localCopiedFiles;
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        if (cleanUp) {
            hiveTestService.cleanUp();
            super.cleanUp();
        }
    }

    /**
     * Initialize input configs.
     *
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void initializeInputConfigs() throws IOException {

        inputConfiguration = new HashMap<String, String>();

        //cluster
        if (executionMode == ExecutionMode.CLUSTER) {
            copyFilesToHdfs();
            if (StringUtils.isNotBlank(properties.getProperty("hive_ip_table"))) {
                tableIpName = properties.getProperty("hive_ip_table") + random.nextInt(9999);
                final String hdfsFolderLocation = filePath.substring(0, filePath.lastIndexOf("/"));
                final String hive_driver = properties.getProperty("hive_driver");
                CreateTableInDatabase.initializeDatabaseContext(HiveDriver.class.getCanonicalName(), hive_driver, "", "");
                CreateTableInDatabase.createHiveInputTable(inputDataFormat, tableIpName, hdfsFolderLocation + SEPARATOR + "file" + SEPARATOR);
            }
        } else {
            tableIpName = "sales" + random.nextInt(9999);
            hiveTestService.createHiveInputTable(inputDataFormat, tableIpName);

            warehouseDir = hiveTestService.getWarehouse_dir();
            inputConfiguration.put("driver", HIVE_URL);
        }

        inputConfiguration.put("uri", "hive://" + tableIpName + FORMAT + getDataFormat(inputDataFormat));
        inputConfiguration.put("name", "Hive_ip_" + getDataFormat(inputDataFormat) + "_" + random.nextInt(9999));
    }

    /**
     * Copy files to hdfs.
     */
    public void copyFilesToHdfs() {
        final String INPUT_FILE = WORKING_DIR + "/data/input_data_set/hive/" + DATA_FILE_NAME + getExtension(inputDataFormat);
        try {
            final String serverFilePath = sshTerminalClientUtil.copyFiles(INPUT_FILE, testScenario, ScenarioType.HDFS);
            filePath = sshTerminalClientUtil.copyFilesOnCluster(serverFilePath, ScenarioType.HDFS);
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Initialize output configs.
     */
    private void initializeOutputConfigs() {
        outputConfiguration = new HashMap<String, String>();

        if (executionMode == ExecutionMode.EMBEDDED) {
            warehouseDir = hiveTestService.getWarehouse_dir();
            tableOpName = "sales_output_" + random.nextInt(9999);
        } else {
            //sandeep cluster
            tableOpName = properties.getProperty("hive_op_table") + random.nextInt(9999);
            warehouseDir = "/user/hive/warehouse/" + tableOpName;
        }

        outputConfiguration.put("name", "Hive_op_" + getDataFormat(outputDataFormat) + "_" + random.nextInt(9999));
        outputConfiguration.put("uri", "hive://" + tableOpName + FORMAT + getDataFormat(outputDataFormat));
    }
}
