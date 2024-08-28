package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.INPUT_DATA_FOLDER;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.test.cluster.util.SshTerminalClientUtil;
import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.ericsson.component.aia.services.bps.test.enums.TestType;
import com.ericsson.component.aia.services.bps.test.mock.embedded.services.MockHdfsClusterService;

/**
 * TestHdfsContext helps in creating/connecting to Hdfs server. It also validates Expected vs Actual outputs.
 */
public class TestHdfsContext extends TestBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestHdfsContext.class);

    /** The input file. */
    private String inputFile;

    /** The output folder. */
    private String outputFolder;

    /** The hdfs uri output. */
    private String hdfsUriOutput;

    /** The fs. */
    private FileSystem fs;

    /** The conf. */
    private Configuration conf;

    /** The mock hdfs cluster. */
    private MockHdfsClusterService mockHdfsCluster;

    /** The hdfs URL. */
    private String hdfsURL;

    private static String FILE_PATH = null;

    private SshTerminalClientUtil sshTerminalClientUtil;

    /**
     * Instantiates a new mock hdfs context.
     *
     * @param testScenario
     *            the test scenario
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public TestHdfsContext(final String testScenario) throws IllegalArgumentException, IOException {
        super(testScenario);
        this.testScenario = testScenario;

        outputFolder = "/hadoop-op" + "_" + getCurrentTime();
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);

        if (executionMode == ExecutionMode.EMBEDDED) {
            mockHdfsCluster = MockHdfsClusterService.getInstance(testScenario);
            hdfsURL = "hdfs://localhost:" + mockHdfsCluster.getHdfsCluster().getNameNodePort();
            fs = mockHdfsCluster.getFileSys();
            conf = mockHdfsCluster.getConf();

        } else {

            conf = new Configuration();
            conf.set("fs.default.name", properties.getProperty("fs.defaultFS"));
            hdfsURL = properties.getProperty("hdfsURL");
            fs = FileSystem.get(conf);
            sshTerminalClientUtil = new SshTerminalClientUtil();
            sshTerminalClientUtil.setProperties(properties);
        }
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
    }

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> getOutputConfigurations() {

        if (outputConfiguration == null) {
            hdfsUriOutput = hdfsURL + outputFolder;
            initializeOutputConfigs(TestType.HDFS.name(), hdfsUriOutput);
        }
        return outputConfiguration;
    }

    /**
     * Validates expected and actual output data.
     */
    @Override
    public void validate() {

        final File localDownloadFiles = copyFromHdfsToLocal();

        try {
            validateFileOutput(localDownloadFiles.getAbsoluteFile().getPath(),
                    EXPECTED_OUTPUT_DIR + SEPARATOR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
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

        if (null != fs) {
            try {
                fs.close();
            } catch (final IOException e) {
                LOGGER.debug("CleanUp, IOException", e);
            }
        }

        if (null != conf) {
            conf = null;
        }

        if (cleanUp) {

            if (executionMode == ExecutionMode.EMBEDDED) {
                mockHdfsCluster.cleanUp();
            }
            super.cleanUp();
        }
    }

    /**
     * Copy to local.
     *
     * @return the file
     */
    private File copyFromHdfsToLocal() {
        final FSDataInputStream in = null;
        File file;
        try {

            if (executionMode == ExecutionMode.EMBEDDED) {
                file = new File(tmpDir.toAbsolutePath() + Constants.SEPARATOR + "hdfs-op" + getTestScenarioName());
                file.mkdirs();
                final FileStatus[] status = fs.listStatus(new Path(outputFolder));
                for (int i = 0; i < status.length; i++) {
                    fs.copyToLocalFile(status[i].getPath(),
                            new Path("file:///" + file.getAbsoluteFile().getPath() + Constants.SEPARATOR + status[i].getPath().getName()));
                }

            } else {
                file = new File(tmpDir.toAbsolutePath() + Constants.SEPARATOR + "hdfs-op_" + getCurrentTime() + Constants.SEPARATOR);
                file.mkdirs();
                final FileStatus[] status = fs.listStatus(new Path(outputFolder));
                for (int i = 0; i < status.length; i++) {
                    sshTerminalClientUtil.copyFilesFromFSToCluster(status[i].getPath(), file.getAbsoluteFile().getPath() + Constants.SEPARATOR,
                            ScenarioType.HDFS);
                }
                Thread.sleep(3000);
                file = sshTerminalClientUtil.copyFilesFromClusterToLocal(file.getAbsoluteFile().getPath() + Constants.SEPARATOR, ScenarioType.HDFS);
            }
        } catch (final Exception e) {
            fail(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeStream(in);
        }
        return file;
    }

    /**
     * Copy files to hdfs.
     */
    public void copyFilesToHdfs() {
        final String INPUT_FILE = INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(inputDataFormat);
        inputFile = SEPARATOR + DATA_FILE_NAME + getExtension(inputDataFormat);
        try {
            // Input stream for the file in local file system to be written to
            // HDFS
            if (executionMode == ExecutionMode.EMBEDDED) {
                final InputStream in = new BufferedInputStream(new FileInputStream(INPUT_FILE));
                // Destination file in HDFS
                //fs = FileSystem.get(URI.create(inputFile), conf);
                final OutputStream out = fs.create(new Path(inputFile));
                // Copy file from local to HDFS
                IOUtils.copyBytes(in, out, 4096, true);
            } else {
                final String clusterFilePath = sshTerminalClientUtil.copyFiles(INPUT_FILE, testScenario, ScenarioType.HDFS);
                FILE_PATH = sshTerminalClientUtil.copyFilesOnCluster(clusterFilePath, ScenarioType.HDFS);
            }
            LOGGER.debug(inputFile + " copied to HDFS");
        } catch (final IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Initialize input configs.
     */
    private void initializeInputConfigs() {
        copyFilesToHdfs();
        String hdfsURI = "";
        if (executionMode == ExecutionMode.EMBEDDED) {
            hdfsURI = hdfsURL + inputFile;
        } else {
            hdfsURI = hdfsURL + FILE_PATH;
        }
        initializeInputConfigs(TestType.HDFS.name(), hdfsURI);
    }
}
