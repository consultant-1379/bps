/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.core.common.Constants.APP_NAME;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MASTER_URL;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MODE;
import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_CSV_DATA_SET;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.TABLE_HEADER;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.TRUE;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.fileFilter;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.DataFormat;
import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.test.common.TestUtil;
import com.ericsson.component.aia.services.bps.test.common.util.JdbcDataBaseUtility;
import com.google.gson.JsonSyntaxException;

import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

/**
 * BaseMockContext is the parent base class for all mock context objects and it holds common code useful while running test case.
 */
public abstract class TestBaseContext {

    /** The tmp dir. */
    protected Path tmpDir;

    /** The input data format. */
    protected String inputDataFormat;

    /** The output data format. */
    protected String outputDataFormat;

    /** The random. */
    protected Random random = new Random();

    /** The input configuration. */
    protected Map<String, String> inputConfiguration;

    /** The output configuration. */
    protected Map<String, String> outputConfiguration;

    /** The execution mode. */
    protected ExecutionMode executionMode;

    /** The test scenario. */
    protected String testScenario;

    /** The properties. */
    protected Properties properties;

    /** The counter. */
    private static int counter = 0;
    /** The Constant CSV_DELIMITER. */
    public static final String CSV_DELIMITER = ",";

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestBaseContext.class);

    /** The clean up. */
    protected static boolean cleanUp = true;

    protected static final String FORMAT = "?format=";

    /**
     * Instantiates a new base mock context.
     *
     * @param testScenarioPath
     *            the test scenario path
     */
    public TestBaseContext(final String testScenarioPath) {
        final File fileDir = new File(testScenarioPath);

        if (fileDir.exists()) {
            fileDir.delete();
        }
        fileDir.mkdir();
        tmpDir = fileDir.toPath();

        properties = TestUtil.getProperties("testing.properties");
        executionMode = ExecutionMode.valueOf(properties.getProperty("executionMode"));
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     * @throws Exception
     *             the exception
     */
    public abstract Map<String, String> getInputConfigurations() throws Exception;

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     * @throws Exception
     *             the exception
     */
    public abstract Map<String, String> getOutputConfigurations() throws Exception;

    /**
     * Validates test case scenario.
     *
     * @throws Exception
     *             the exception
     */
    public abstract void validate() throws Exception;

    /**
     * StepConfigurations for a Job as defined in flow xml.
     *
     * @return the map
     */
    public Map<String, String> getStepConfigurations() {

        final Map<String, String> stepMap = new HashMap<String, String>();

        stepMap.put("sql", "SELECT TRANSACTION_DATE,PRODUCT,PRICE,PAYMENT_TYPE,NAME,"
                + "CITY,STATE,COUNTRY,ACCOUNT_CREATED,LAST_LOGIN,LATITUDE,LONGITUDE FROM sales");

        if (executionMode == ExecutionMode.EMBEDDED) {
            stepMap.put(MODE, executionMode.name());
            stepMap.put(MASTER_URL, "local[*]");
            stepMap.put(APP_NAME, "testing" + new Random().nextInt(99999));
            stepMap.put("uri", PROCESS_URIS.SPARK_BATCH_SQL.getUri() + "sales-analysis" + random.nextInt(99999));
        } else {
            stepMap.put(MODE, executionMode.name());
            stepMap.put("master.url", properties.getProperty("master.url"));
            stepMap.put("uri", PROCESS_URIS.SPARK_BATCH_SQL.getUri() + "sales-analysis" + random.nextInt(99999));
        }

        for (final Map.Entry<String, String> entry : getConfigMap().entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
            // stepMap.put(entry.getKey(), entry.getValue());
        }

        return stepMap;
    }

    /**
     * Clean up operation for junit test cases.
     */
    public void cleanUp() {

        if (cleanUp) {
            deleteFolder(tmpDir.toFile());

            for (final Map.Entry<String, String> entry : getConfigMap().entrySet()) {
                deleteFolder(new File(entry.getValue()));
            }
        }
    }

    /**
     * Delete folder.
     *
     * @param file
     *            the file
     */
    protected void deleteFolder(final File file) {
        try {
            FileDeleteStrategy.FORCE.delete(file);
        } catch (final IOException e) {
            LOGGER.debug("CleanUp, IOException", e);
        }
    }

    /**
     * Gets the filtered actual files.
     *
     * @param outputDirectory
     *            the output directory
     * @return the filtered actual files
     */
    protected File[] getFilteredActualFiles(final String outputDirectory) {

        final File[] actual = new File(outputDirectory).listFiles(fileFilter);
        Arrays.sort(actual);
        return actual;
    }

    /**
     * Validates expected and actual file output data.
     *
     * @param outputDirectory
     *            the outputDirectory
     * @param expectedOpPath
     *            the expected output path
     * @param dataFormat
     *            the data format
     * @throws Exception
     *             the exception
     * @throws JsonSyntaxException
     *             the json syntax exception
     */

    public void validateFileOutput(final String outputDirectory, final String expectedOpPath, final String dataFormat) throws Exception {

        final File expectedFile;
        final File[] generatedOutputFiles = getFilteredActualFiles(outputDirectory);
        final File actualSingleOutputFile;
        if (DataFormat.PARQUET == DataFormat.valueOf(dataFormat)) {
            final File csvOutputFile = new File(outputDirectory + SEPARATOR + "converted_csv_" + getCurrentTime());

            for (final File tmpFile : generatedOutputFiles) {
                convertParquetToCSV(tmpFile, csvOutputFile);
            }
            actualSingleOutputFile = csvOutputFile;
            expectedFile = new File(EXPECTED_CSV_DATA_SET);
        } else {
            actualSingleOutputFile = new File(outputDirectory + getCurrentTime());
            expectedFile = new File(expectedOpPath);
            TestUtil.joinFiles(actualSingleOutputFile, generatedOutputFiles);
        }

        containsExactText(expectedFile, actualSingleOutputFile);
    }

    /**
     * Validates expected and actual DB output data.
     *
     * @param conn
     *            the connection
     * @param dbName
     *            the database name
     * @param query
     *            the query
     * @param expectedFilePath
     *            the expected file path
     */
    public void validateDBOutput(final Connection conn, final String dbName, String query, final String expectedFilePath) {

        final String JDBC_OP = tmpDir.toAbsolutePath() + SEPARATOR + dbName + "_JDBC_OP.csv";
        query = query.replace("JDBC_OP", JDBC_OP);
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            conn.close();
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }

        final File expectedFile = new File(expectedFilePath);
        final File actualFile = new File(JDBC_OP);

        try {
            containsExactText(expectedFile, actualFile);
        } catch (final IOException e) {

            try {
                LOGGER.debug("FileUtils.readLines(actualFile)----" + FileUtils.readLines(actualFile));
                LOGGER.debug("FileUtils.readLines(expectedFile)----" + FileUtils.readLines(expectedFile));
            } catch (final IOException e1) {
                LOGGER.debug("validateDBOutput, IOException", e);
            }

            Assert.fail(e.getMessage());
        }
    }

    public void validateClusterDBOutput(final Connection connection, final String tableName, final String expectedFilePath) throws SQLException {
        final String JDBC_OP = tmpDir.toAbsolutePath() + SEPARATOR + "postgres" + "_JDBC_OP.csv";

        try {
            JdbcDataBaseUtility.writeDataIntoFileFromJdbcDataBase(connection, tableName, JDBC_OP);
            final File expectedFile = new File(expectedFilePath);
            final File actualFile = new File(JDBC_OP);
            containsExactText(expectedFile, actualFile);

        } catch (final SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } catch (final IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Gets the extension.
     *
     * @param dataFormat
     *            the data format
     * @return the extension
     */
    protected String getExtension(final String dataFormat) {
        return "." + DataFormat.valueOf(dataFormat).dataFormat;
    }

    /**
     * Gets the config map.
     *
     * @return the config map
     */
    private Map<String, String> getConfigMap() {

        final Map<String, String> sparkConfigMap = new HashMap<>();
        final String tmpFolder = tmpDir.toAbsolutePath() + SEPARATOR;
        sparkConfigMap.put("spark.local.dir", tmpFolder + "spark_local_dir");
        sparkConfigMap.put("hive.exec.dynamic.partition.mode", "nonstrict");
        sparkConfigMap.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConfigMap.put("spark.externalBlockStore.url", tmpFolder + "spark.externalBlockStore.url");
        sparkConfigMap.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConfigMap.put("hive.metastore.warehouse.dir", tmpFolder + "hive");
        sparkConfigMap.put("spark.externalBlockStore.baseDir", tmpFolder + "spark.externalBlockStore.baseDir");
        sparkConfigMap.put("hive.exec.scratchdir", tmpFolder + "hive.exec.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + getCurrentTime());
        sparkConfigMap.put("hive.querylog.location", tmpFolder + "querylog.location");
        sparkConfigMap.put("hive.exec.local.scratchdir", tmpFolder + "hive.exec.local.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpFolder + "hive.downloaded.resources.dir");
        sparkConfigMap.put("hive.metadata.export.location", tmpFolder + "hive.metadata.export.location");
        sparkConfigMap.put("hive.metastore.metadb.dir", tmpFolder + "hive.metastore.metadb.dir");
        sparkConfigMap.put("hive.merge.sparkfiles", "true");
        return sparkConfigMap;
    }

    /**
     * Removes the header if exits.
     *
     * @param actualFile
     *            the actual file
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<String> removeHeaderIfExits(final File actualFile) throws IOException {

        final List<String> actualLines = FileUtils.readLines(actualFile);
        final List<String> removeHeaderlist = new ArrayList<String>();

        for (int i = 0; i < 2; ++i) {

            if (actualLines.get(i).contains(TABLE_HEADER)) {
                removeHeaderlist.add(actualLines.get(i));
            }
        }
        actualLines.removeAll(removeHeaderlist);
        return actualLines;
    }

    /**
     * Contains exact text.
     *
     * @param expectedFile
     *            the expected file
     * @param actualFile
     *            the actual file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void containsExactText(final File expectedFile, final File actualFile) throws IOException {

        final List<String> actualLines = removeHeaderIfExits(actualFile);
        final List<String> expectedLines = FileUtils.readLines(expectedFile);

        Collections.sort(actualLines);
        Collections.sort(expectedLines);
        Assert.assertThat(expectedLines, Matchers.containsInAnyOrder(actualLines.toArray()));
    }

    /**
     * Gets the data format.
     *
     * @param dataFormat
     *            the data format
     * @return the data format
     */
    protected String getDataFormat(final String dataFormat) {
        return DataFormat.valueOf(dataFormat).dataFormat;
    }

    /**
     * Convert parquet to CSV.
     *
     * @param parquetFile
     *            the parquet file
     * @param csvOutputFile
     *            the csv output file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("deprecation")
    public static void convertParquetToCSV(final File parquetFile, final File csvOutputFile) throws IOException {

        final org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path(parquetFile.toURI());
        final Configuration configuration = new Configuration(true);
        final GroupReadSupport readSupport = new GroupReadSupport();
        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
        final MessageType schema = readFooter.getFileMetaData().getSchema();
        readSupport.init(configuration, null, schema);
        final BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile, true));
        final ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);

        try {
            Group g = null;
            while ((g = reader.read()) != null) {
                writeGroup(w, g, schema);
            }
            reader.close();
        } finally {
            closeQuietly(w);
        }
    }

    /**
     * Write group.
     *
     * @param writer
     *            the writer
     * @param grp
     *            the grp
     * @param schema
     *            the schema
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void writeGroup(final BufferedWriter writer, final Group grp, final MessageType schema) throws IOException {
        for (int j = 0; j < schema.getFieldCount(); j++) {
            if (j > 0) {
                writer.write(CSV_DELIMITER);
            }
            final String valueToString = grp.getValueToString(j, 0);
            writer.write(valueToString);
        }
        writer.write('\n');
    }

    /**
     * Close quietly.
     *
     * @param res
     *            the res
     */
    public static void closeQuietly(final Closeable res) {
        try {
            if (res != null) {
                res.close();
            }
        } catch (final IOException ioe) {
            LOGGER.debug("Exception closing reader " + res + ": " + ioe.getMessage());
        }
    }

    protected static String getFileUri() {
        if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_UNIX) {
            return IOURIS.FILE.getUri();
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return IOURIS.FILE.getUri() + "/";
        } else {
            throw new BpsRuntimeException("OS not supported..Supported OS - LINUX/UNIX/WINDOWS");
        }
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
     * Gets the temporary directory.
     *
     * @return the tmpDir
     */
    public Path getTmpDir() {
        return tmpDir;
    }

    /**
     * Gets the current time.
     *
     * @return the current time
     */
    protected static String getCurrentTime() {
        ++counter;
        final SimpleDateFormat format = new SimpleDateFormat("dd_M_yyyy_hh_mm_ss");
        return format.format(new Date()) + "_" + counter;

    }

    /**
     * Sets the csv props if exists.
     */
    protected void setCsvPropsIfExists() {

        if (DataFormat.valueOf(inputDataFormat) == DataFormat.CSV) {
            inputConfiguration.put("header", TRUE);
            inputConfiguration.put("inferSchema", "true");
            inputConfiguration.put("drop-malformed", TRUE);
            inputConfiguration.put("skip-comments", TRUE);
            inputConfiguration.put("quoteMode", "ALL");
        }
    }

    /**
     * Sets the xml props if exists.
     */
    protected void setXmlPropsIfExists() {

        if (DataFormat.valueOf(inputDataFormat) == DataFormat.XML) {
            inputConfiguration.put("header", TRUE);
            inputConfiguration.put("inferSchema", "true");
            inputConfiguration.put("drop-malformed", TRUE);
            inputConfiguration.put("skip-comments", TRUE);
            inputConfiguration.put("quoteMode", "ALL");
            inputConfiguration.put("rootTag", "DocumentElement");
            inputConfiguration.put("rowTag", "Table1");

        }
    }

    /**
     * Initialize input configs.
     *
     * @param adapterName
     *            the adapter name
     * @param uri
     *            the uri
     */
    protected void initializeInputConfigs(final String adapterName, final String uri) {
        final String dataFormat = getDataFormat(inputDataFormat);
        inputConfiguration = new HashMap<String, String>();
        inputConfiguration.put("uri", uri + FORMAT + dataFormat);
        setCsvPropsIfExists();
        setXmlPropsIfExists();
        final String name = adapterName + "_ip_" + dataFormat + getCurrentTime();
        inputConfiguration.put("table-name", name);
        inputConfiguration.put("name", name);
    }

    /**
     * Initialize output configs.
     *
     * @param adapterName
     *            the adapter name
     * @param uri
     *            the uri
     */
    protected void initializeOutputConfigs(final String adapterName, final String uri) {
        final String dataFormat = getDataFormat(outputDataFormat);
        outputConfiguration = new HashMap<String, String>();
        outputConfiguration.put("uri", uri + FORMAT + dataFormat);

        final String name = adapterName + "_op_" + dataFormat + getCurrentTime();
        outputConfiguration.put("name", name);
    }

    /**
     * Gets the test scenario name with time.
     *
     * @return the test scenario name with time
     */
    protected String getTestScenarioNameWithTime() {
        return "_" + testScenario + getCurrentTime();
    }

    /**
     * Gets the test scenario name.
     *
     * @return the test scenario name
     */
    protected String getTestScenarioName() {
        return "_" + testScenario + "_" + getCurrentTime();
    }
}
