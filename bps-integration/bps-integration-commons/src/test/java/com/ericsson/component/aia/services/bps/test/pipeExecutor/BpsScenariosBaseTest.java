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
package com.ericsson.component.aia.services.bps.test.pipeExecutor;

import static com.ericsson.component.aia.services.bps.core.common.Constants.APP_NAME;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MASTER_URL;
import static com.ericsson.component.aia.services.bps.core.common.Constants.MODE;
import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.BASE_IT_FOLDER;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.ROOT_BASE_FOLDER;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.ericsson.component.aia.services.bps.core.common.DataFormat;
import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.test.cluster.util.SshTerminalClientUtil;
import com.ericsson.component.aia.services.bps.test.common.TestUtil;
import com.ericsson.component.aia.services.bps.test.enums.ScenarioType;
import com.ericsson.component.aia.services.bps.test.enums.TestType;
import com.ericsson.component.aia.services.bps.test.pipeExecutor.beans.TestAdapterBean;
import com.ericsson.component.aia.services.bps.test.pipeExecutor.beans.TestScenarioDetailsBean;
import com.ericsson.component.aia.services.bps.test.services.TestBaseContext;
import com.ericsson.component.aia.services.bps.test.services.TestHiveContext;
import com.ericsson.component.aia.services.bps.test.services.TestJdbcContext;

/**
 * Integration test suite for BPSPipeLineExecuter.
 */
@Ignore
public abstract class BpsScenariosBaseTest {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(BpsScenariosBaseTest.class);

    /** The Constant emptyArray. */
    private static final String[] emptyArray = { "" };

    /** The tmp dir. */
    protected Path tmpDir;

    /** The input adapters. */
    protected Set<TestAdapterBean> inputAdapters;

    /** The output adapters. */
    protected Set<TestAdapterBean> outputAdapters;

    /** The output contexts. */
    final protected List<TestBaseContext> outputContexts = new ArrayList<>();

    /** The test scenario. */
    protected String testScenario;

    /** The counter. */
    protected static int counter = 0;

    protected boolean oneToManyScenario = false;

    public Properties properties;

    private SshTerminalClientUtil sshTerminalClientUtil;

    private ScenarioType scenarioType;

    /**
     * Instantiates a new BPS pipe line executor batch test.
     *
     * @param testScenarioBean
     *            the test scenario bean
     */
    public BpsScenariosBaseTest(final TestScenarioDetailsBean testScenarioBean) {

        this.inputAdapters = testScenarioBean.getInputAdapters();
        this.outputAdapters = testScenarioBean.getOutputAdapters();

        final Iterator<TestAdapterBean> setIterator = inputAdapters.iterator();
        while (setIterator.hasNext()) {
            this.testScenario = setIterator.next().getAdapterType().getSimpleName();
            break;
        }
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @param fromType
     *            the from type
     *
     * @return the collection
     * @throws Exception
     *             the exception
     */
    public static List<Object> provideData(final TestType fromType) throws Exception {

        final TestScenarioDetailsBean testScenarioBean = new TestScenarioDetailsBean();

        for (final String inputDataFormat : getDataFormatScenarios(fromType.getInputDataFormats())) {
            testScenarioBean.getInputAdapters().add(new TestAdapterBean(inputDataFormat, fromType.ref));
        }

        for (final TestType toType : TestType.values()) {

            //single scenario
            if (fromType != toType) {
                continue;
            }

            for (final String outputDataFormat : getDataFormatScenarios(toType.getOutputDataFormats())) {
                testScenarioBean.getOutputAdapters().add(new TestAdapterBean(outputDataFormat, toType.ref));
            }
        }

        LOGGER.debug("Finished created integration test scenarios");

        return Arrays.asList(new Object[] { testScenarioBean });
    }

    public static List<Object> provideKafkaData(final TestType toType11, final String testScenario) throws Exception {

        final TestScenarioDetailsBean testScenarioBean = new TestScenarioDetailsBean();
        testScenarioBean.setTestScenario(testScenario);

        for (final String inputDataFormat : getDataFormatScenarios(TestType.FILE.getInputDataFormats())) {

            testScenarioBean.getInputAdapters().add(new TestAdapterBean(inputDataFormat, TestType.FILE.ref));
        }

        for (final TestType toType : TestType.values()) {
            for (final String outputDataFormat : getDataFormatScenarios(toType.getOutputDataFormats())) {
                testScenarioBean.getOutputAdapters().add(new TestAdapterBean(outputDataFormat, toType.ref));
            }
        }

        LOGGER.debug("Finished created integration test scenarios");
        return Arrays.asList(new Object[] { testScenarioBean });
    }

    public static List<Object> streamingScenarios(final TestType fromType) throws Exception {

        final TestScenarioDetailsBean testScenarioBean = new TestScenarioDetailsBean();

        for (final String inputDataFormat : getDataFormatScenarios(fromType.getInputDataFormats())) {

            testScenarioBean.getInputAdapters().add(new TestAdapterBean(inputDataFormat, fromType.ref));
        }

        for (final TestType toType : TestType.values()) {

            for (final String outputDataFormat : getDataFormatScenarios(toType.getOutputDataFormats())) {
                testScenarioBean.getOutputAdapters().add(new TestAdapterBean(outputDataFormat, toType.ref));
            }
        }

        LOGGER.debug("Finished created integration test scenarios");
        return Arrays.asList(new Object[] { testScenarioBean });
    }

    protected void validate() throws Exception {
        LOGGER.debug("successfully executed validate");
        for (final TestBaseContext opContext : outputContexts) {
            opContext.validate();
        }
        LOGGER.debug("Finished test scenario");
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @param fromType
     *            the from type
     * @return the collection
     * @throws Exception
     *             the exception
     */
    public static List<Object> provideDataOneToMany(final TestType fromType) throws Exception {

        final List<Object> scenariosList = new ArrayList<>();

        for (final String inputDataFormat : getDataFormatScenarios(fromType.getInputDataFormats())) {

            for (final TestType toType : TestType.values()) {

                //single scenario
                if (fromType != toType) {
                    continue;
                }

                for (final String outputDataFormat : getDataFormatScenarios(toType.getOutputDataFormats())) {
                    final TestScenarioDetailsBean testScenarioBean = new TestScenarioDetailsBean();
                    testScenarioBean.getInputAdapters().add(new TestAdapterBean(inputDataFormat, fromType.ref));
                    testScenarioBean.getOutputAdapters().add(new TestAdapterBean(outputDataFormat, toType.ref));
                    scenariosList.add(testScenarioBean);
                }
            }
        }

        LOGGER.debug("Finished created integration test scenarios");
        return scenariosList;
    }

    /**
     * Gets test scenarios for data formats.
     *
     * @param dataFormats
     *            the data formats
     * @return the scenarios
     */
    private static String[] getDataFormatScenarios(final EnumSet<DataFormat> dataFormats) {

        if (!dataFormats.isEmpty()) {

            final List<String> lst = new ArrayList<String>();
            for (final DataFormat dataFormat : dataFormats) {
                lst.add(dataFormat.name());
            }
            return Arrays.copyOf(lst.toArray(), lst.toArray().length, String[].class);

        }
        return emptyArray;
    }

    /**
     * Creates the base folder.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void createBaseFolder() throws Exception {

        if (!new File(BASE_IT_FOLDER).exists()) {
            FileUtils.forceMkdir(new File(BASE_IT_FOLDER));
        }
    }

    /**
     * Delete base folder.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void deleteBaseFolder() throws Exception {

        //        if (new File(BASE_IT_FOLDER).exists()) {
        //            FileUtils.forceDelete(new File(BASE_IT_FOLDER));
        //        }
    }

    /**
     * Creates and initializes folders and different context required for the unit test case.
     *
     * @throws Exception
     *             the exception
     */
    public void setUp() throws Exception {

        final String tmpStr = getCurrentTime(testScenario);

        testScenario = ROOT_BASE_FOLDER + "testing_scenarios_for_" + tmpStr;

        final File fileDir = new File(testScenario);
        fileDir.mkdir();
        tmpDir = fileDir.toPath();
        LOGGER.debug("Created Temp Directory--" + tmpDir.toAbsolutePath());
        System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");
        System.setProperty("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + testScenario);

    }

    public void setProperties(final ScenarioType scenarioType) {
        properties = TestUtil.getProperties("testing.properties");
        final ExecutionMode executionMode = ExecutionMode.valueOf(properties.getProperty("executionMode"));
        if (executionMode == ExecutionMode.CLUSTER) {
            sshTerminalClientUtil = new SshTerminalClientUtil();
            sshTerminalClientUtil.setProperties(properties);
            this.scenarioType = scenarioType;
        }
    }

    /**
     * Executes SubmitJobStatus scenario {0} based on Collection{Object[]}.
     *
     * @throws Exception
     *             the exception
     * @throws SecurityException
     *             the security exception
     */
    public void testScenario() throws Exception {
        setUp();
        /*
         * System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");
         */
        final List<Map<String, String>> ipAdatperlist = new ArrayList<>();
        final List<Map<String, String>> opAdatperlist = new ArrayList<>();
        final List<Map<String, String>> steplist = new ArrayList<>();

        StringBuilder joinTables = null;
        int index = 0;

        for (final TestAdapterBean ipAdapter : inputAdapters) {
            final Constructor<? extends TestBaseContext> constructor = ipAdapter.getAdapterType().getDeclaredConstructor(String.class);
            final TestBaseContext inputContext = constructor.newInstance(testScenario);
            inputContext.setInputDataFormat(ipAdapter.getDataFormat());
            final Map<String, String> inputConfigurations = inputContext.getInputConfigurations();
            final String tableName;

            if (ipAdapter.getAdapterType() == TestHiveContext.class) {
                final Properties prop = new Properties();
                prop.putAll(inputConfigurations);
                final URIDefinition<IOURIS> decode = IOURIS.decode(prop);
                tableName = decode.getContext();
            } else if (ipAdapter.getAdapterType() == TestJdbcContext.class) {
                tableName = inputConfigurations.get("table.name");
            } else {
                tableName = inputConfigurations.get("table-name") + index;
                inputConfigurations.put("table-name", tableName);
            }

            final String currentTable = " t" + index;

            if (joinTables == null) {
                joinTables = new StringBuilder(tableName + currentTable);
            } else {
                joinTables.append(" UNION SELECT DISTINCT ID,TRANSACTION_DATE,PRODUCT,PRICE,PAYMENT_TYPE,NAME,"
                        + "CITY,STATE,COUNTRY,ACCOUNT_CREATED,LAST_LOGIN,LATITUDE,LONGITUDE FROM " + tableName + currentTable);
            }

            ipAdatperlist.add(inputConfigurations);
            ++index;
        }

        for (final TestAdapterBean opAdapter : outputAdapters) {
            final Constructor<? extends TestBaseContext> constructor = opAdapter.getAdapterType().getDeclaredConstructor(String.class);
            final TestBaseContext outputContext = constructor.newInstance(testScenario);
            outputContext.setOutputDataFormat(opAdapter.getDataFormat());
            opAdatperlist.add(outputContext.getOutputConfigurations());
            outputContexts.add(outputContext);
        }

        final Map<String, String> stepMap = new HashMap<String, String>();
        final ExecutionMode executionMode = ExecutionMode.valueOf(properties.getProperty("executionMode"));
        stepMap.put(MODE, executionMode.name());
        stepMap.put("uri", PROCESS_URIS.SPARK_BATCH_SQL.getUri() + "sales-analysis" + counter);
        stepMap.put("sql", "SELECT DISTINCT ID,TRANSACTION_DATE,PRODUCT,PRICE,PAYMENT_TYPE,NAME,"
                + "CITY,STATE,COUNTRY,ACCOUNT_CREATED,LAST_LOGIN,LATITUDE,LONGITUDE FROM " + joinTables);
        if (executionMode == ExecutionMode.EMBEDDED) {
            stepMap.put(MASTER_URL, "local[*]");
            stepMap.put(APP_NAME, "testing" + new Random().nextInt(99999));
        } else {
            stepMap.put("master.url", properties.getProperty("master.url"));
        }
        stepMap.put("spark.sql.shuffle.partitions", "1");

        steplist.add(stepMap);

        final Map<String, List<Map<String, String>>> context = new HashMap<>();
        context.put("ipAdatperlist", ipAdatperlist);
        context.put("opAdatperlist", opAdatperlist);
        context.put("attributeStepMap", steplist);

        LOGGER.debug("About to execute pipeline");

        executePipeLine(context, tmpDir, executionMode);

        LOGGER.debug("successfully executed pipeline");

        for (final TestBaseContext opContext : outputContexts) {
            opContext.validate();
        }

        LOGGER.debug("successfully executed validate");
        LOGGER.debug("Finished test scenario");
    }

    public abstract void executePipeLine(final Map<String, List<Map<String, String>>> context, final Path target_op,
                                         final ExecutionMode executionMode);

    public String copyFilesOnCLuster(final String itemToCopy) {
        final String filePath = sshTerminalClientUtil.copyFiles(itemToCopy, testScenario, ScenarioType.HDFS);
        return sshTerminalClientUtil.copyFilesOnCluster(filePath, ScenarioType.HDFS);
    }

    /**
     * Clean up operation for junit test cases.
     */
    @After
    public void tearDown() {
        LOGGER.debug("Cleaning up of junit started");

        final ExecutionMode executionMode = ExecutionMode.valueOf(properties.getProperty("executionMode"));
        if (executionMode.equals(ExecutionMode.CLUSTER)) {
            switch (scenarioType) {
                case ALLUXIO:
                    sshTerminalClientUtil.cleanUpFromCluster(testScenario, scenarioType);
                    sshTerminalClientUtil.cleanUpFromCluster(testScenario, ScenarioType.HDFS);
                    break;
                case HDFS:
                case HIVE:
                case JDBC:
                    sshTerminalClientUtil.cleanUpFromCluster(testScenario, ScenarioType.HDFS);
                    break;
                default:
                    break;
            }

        }

        LOGGER.debug("Cleanup of junit is successful");

    }

    /**
     * Gets the current time.
     *
     * @param scenario
     *            the scenario
     * @return the current time
     */
    protected static String getCurrentTime(final String scenario) {
        ++counter;
        final SimpleDateFormat format = new SimpleDateFormat("dd_M_yyyy_hh_mm_ss");
        return format.format(new Date()) + "_" + scenario + "_run";
    }
}
