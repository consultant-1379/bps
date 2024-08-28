/*------------------------------------------------------------------------------
 *******************************************************************************
q * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.INPUT_DATA_FOLDER;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.test.cluster.util.SshTerminalClientUtil;
import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.ericsson.component.aia.services.bps.test.mock.embedded.services.MockAlluxioTestService;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

/**
 * TestAlluxioContext helps in creating/connecting to alluxio server. It also validates Expected vs Actual outputs.
 */
public class TestAlluxioContext extends TestBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestAlluxioContext.class);

    /** The mock alluxio cluster. */
    private MockAlluxioTestService mockAlluxioCluster;

    /** The alluxio uri output. */
    private String alluxioUriOutput = null;

    private SshTerminalClientUtil sshTerminalClientUtil = null;

    /**
     * Instantiates a new mock alluxio context test.
     *
     * @param testScenario
     *            the test scenario
     * @throws Exception
     *             the exception
     */
    public TestAlluxioContext(final String testScenario) throws Exception {
        super(testScenario);
        this.testScenario = testScenario;
        if (executionMode == ExecutionMode.EMBEDDED) {
            mockAlluxioCluster = MockAlluxioTestService.getInstance(testScenario);
        } else {
            sshTerminalClientUtil = new SshTerminalClientUtil();
            sshTerminalClientUtil.setProperties(properties);
        }

    }

    /**
     * List files.
     *
     * @param fs
     *            the fs
     * @param path
     *            the path
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static List<String> listFiles(final FileSystem fs, final String path) throws IOException {
        try {
            final List<URIStatus> statuses = fs.listStatus(new AlluxioURI(path));
            final List<String> res = new ArrayList<>();
            for (final URIStatus status : statuses) {
                res.add(status.getPath());
                if (status.isFolder()) {
                    res.addAll(listFiles(fs, status.getPath()));
                }
            }
            return res;
        } catch (final AlluxioException e) {
            throw new IOException(e.getMessage());
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
            initalizeIpMap();
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
            initialOpMap();
        }
        return outputConfiguration;
    }

    /**
     * Validate.
     *
     * @throws Exception
     *             the exception
     */
    @Override
    public void validate() throws Exception {
        final FileSystem fs = FileSystem.Factory.get();
        final File localDownloadFiles = copyFilesToLocal(fs);
        try {
            validateFileOutput(localDownloadFiles.getAbsoluteFile().getPath(),
                    EXPECTED_OUTPUT_DIR + SEPARATOR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
        } catch (final Exception e) {
            LOGGER.info("validate operation failed got Exception", e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        if (cleanUp) {
            mockAlluxioCluster.cleanUp();
            super.cleanUp();
        }
    }

    /**
     * Copy file to local.
     *
     * @param file
     *            the file
     * @param filePath
     *            the file path
     * @param fs
     *            the fs
     */
    private void copyFileToLocal(final File file, final String filePath, final FileSystem fs) {
        OutputStream outputStream = null;
        final AlluxioURI alluxioPath = new AlluxioURI(alluxioUriOutput);
        final AlluxioURI path = new AlluxioURI(IOURIS.ALLUXIO.getUri() + alluxioPath.getHost() + ":" + alluxioPath.getPort() + filePath);

        // Open the file for reading and obtains a lock preventing deletion
        FileInStream in = null;

        try {
            in = fs.openFile(path);
            // write the inputStream to a FileOutputStream
            outputStream = new FileOutputStream(file);

            int read = 0;
            final byte[] bytes = new byte[1024];

            while ((read = in.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }

        } catch (final Exception e) {
            LOGGER.info(e);
        } finally {

            if (outputStream != null) {
                try {
                    outputStream.flush();
                    outputStream.close();
                } catch (final IOException e) {
                    LOGGER.info(e);
                }

            }
            if (in != null) {
                try {
                    outputStream.flush();
                    in.close();
                } catch (final IOException e) {
                    LOGGER.info(e);
                }
            }
        }
    }

    /**
     * Copy files to local.
     *
     * @param fs
     *            the fs
     * @return the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public File copyFilesToLocal(final FileSystem fs) throws IOException {
        File file = new File(tmpDir.toAbsolutePath() + Constants.SEPARATOR + "alluxio-op_" + getCurrentTime());

        try {
            if (!file.exists()) {
                file.mkdirs();
            }

            if (executionMode == ExecutionMode.EMBEDDED) {

                final List<String> filesList = listFiles(fs, alluxioUriOutput);

                for (final String filePath : filesList) {
                    final String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                    copyFileToLocal(new File(file.getAbsolutePath() + SEPARATOR + fileName), filePath, fs);
                }
            } else {
                final File localCopiedFiles;
                final String outputPath = testScenario + SEPARATOR + "output" + SEPARATOR;
                final Path path = new Path(outputPath);
                sshTerminalClientUtil.copyFilesFromFSToCluster(path, file.getAbsoluteFile().getPath() + Constants.SEPARATOR, ScenarioType.ALLUXIO);
                Thread.sleep(3000);
                localCopiedFiles = sshTerminalClientUtil.copyFilesFromClusterToLocal(file.getAbsoluteFile().getPath() + Constants.SEPARATOR,
                        ScenarioType.ALLUXIO);
                if (outputDataFormat.equalsIgnoreCase("JSON")) {
                    file = new File(localCopiedFiles + SEPARATOR + "json" + SEPARATOR);
                } else {
                    file = new File(localCopiedFiles + SEPARATOR + "csv" + SEPARATOR);
                }

            }
        } catch (final Exception e) {
            fail(e.getMessage());
            throw new RuntimeException(e);
        }

        return file;
    }

    /**
     * Initalize ip map.
     */
    private void initalizeIpMap() {
        String alluxioURI = null;
        inputConfiguration = new HashMap<String, String>();
        final String dataFormat = getDataFormat(inputDataFormat);

        if (executionMode == ExecutionMode.EMBEDDED) {

            mockAlluxioCluster.copyFiles(inputDataFormat);
            alluxioURI = "alluxio://" + mockAlluxioCluster.getLocalAlluxioCluster().getHostname() + ":"
                    + mockAlluxioCluster.getLocalAlluxioCluster().getMasterRpcPort() + "/" + DATA_FILE_NAME + getExtension(inputDataFormat) + FORMAT
                    + dataFormat;

        } else {
            final String INPUT_FILE = INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(inputDataFormat);
            final String clusterFilePath = sshTerminalClientUtil.copyFiles(INPUT_FILE, testScenario, ScenarioType.ALLUXIO);
            final String filePath = sshTerminalClientUtil.copyFilesOnCluster(clusterFilePath, ScenarioType.ALLUXIO);
            alluxioURI = properties.getProperty("alluxioFileUri") + filePath+ FORMAT
                    + dataFormat;
        }

        inputConfiguration.put("uri", alluxioURI);

        setCsvPropsIfExists();
        setXmlPropsIfExists();

        final String name = "Alluxio_ip_" + dataFormat + "_" + getCurrentTime();

        inputConfiguration.put("table-name", name);
        inputConfiguration.put("name", name);
    }

    /**
     * Initial op map.
     */
    private void initialOpMap() {

        outputConfiguration = new HashMap<String, String>();
        final String dataFormat = getDataFormat(outputDataFormat);

        if (executionMode == ExecutionMode.EMBEDDED) {
            alluxioUriOutput = "alluxio://" + mockAlluxioCluster.getLocalAlluxioCluster().getHostname() + ":"
                    + mockAlluxioCluster.getLocalAlluxioCluster().getMasterRpcPort() + "/" + getCurrentTime() + "/" + FORMAT + dataFormat;
        } else {
            alluxioUriOutput = properties.getProperty("alluxioFileUri") + testScenario + SEPARATOR + "output" + SEPARATOR + dataFormat + SEPARATOR
                    + FORMAT + dataFormat;
        }

        outputConfiguration.put("uri", alluxioUriOutput);

        final String name = "Alluxio_op_" + dataFormat + "_" + getCurrentTime();
        outputConfiguration.put("name", name);
    }
}
