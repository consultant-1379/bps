package com.ericsson.component.aia.services.bps.test.services;

import static com.ericsson.component.aia.services.bps.test.common.TestConstants.EXPECTED_CSV_DATA_SET;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.test.cluster.util.SshTerminalClientUtil;
import com.ericsson.component.aia.services.bps.test.common.util.JdbcDataBaseUtility;
import com.ericsson.component.aia.services.bps.test.mock.embedded.services.MockJdbcTestService;

/**
 * TestJdbcContext helps in creating/connecting to Jdbc server. It also validates Expected vs Actual outputs.
 */
public class TestJdbcContext extends TestBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestJdbcContext.class);

    /** The jdbc test service. */
    private MockJdbcTestService jdbcTestService;

    /** The uri. */
    private String uri = null;

    /** The driver. */
    private String driver = null;

    /** The user name. */
    private String userName = null;

    /** The password. */
    private String password = null;

    /** The ip table name. */
    private String ipTableName = null;

    /** The op table name. */
    private String opTableName = null;

    private SshTerminalClientUtil sshTerminalClientUtil = null;

    /**
     * Instantiates a new mock jdbc context.
     *
     * @param testScenario
     *            the test scenario
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public TestJdbcContext(final String testScenario) throws IOException {

        super(testScenario);
        this.testScenario = testScenario;

        if (executionMode == ExecutionMode.EMBEDDED) {
            driver = properties.getProperty("jdbcDriver");
            userName = properties.getProperty("jdbcUsername");
            password = properties.getProperty("jdbcPassword");

            ipTableName = "sales_ip_" + random.nextInt(9999);
            opTableName = "sales_op_" + random.nextInt(9999);

            jdbcTestService = MockJdbcTestService.getInstance(testScenario);
            jdbcTestService.initializeJdbc(ipTableName);

            uri = IOURIS.JDBC.getUri() + jdbcTestService.getDB_LOCATION();
        } else {
            uri = IOURIS.JDBC.getUri() + properties.getProperty("jdbc_uri");
            driver = properties.getProperty("jdbc_driver");
            ipTableName = properties.getProperty("jdbc_ip_table") + random.nextInt(9999);
            opTableName = properties.getProperty("jdbc_op_table") + random.nextInt(9999);
            userName = properties.getProperty("jdbc_username");
            password = properties.getProperty("jdbc_password");
            sshTerminalClientUtil = new SshTerminalClientUtil();
            sshTerminalClientUtil.setTestDir(testScenario);
            sshTerminalClientUtil.setProperties(properties);
            final String connectionUrl = uri.replace(IOURIS.JDBC.getUri(), "");
            JdbcDataBaseUtility.createTableAndInsertDataInJdbcTable(ipTableName, connectionUrl, userName, password);
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
            initializeOutputConfigs();
        }
        return outputConfiguration;
    };

    /**
     * Validates expected and actual output data.
     */
    @Override
    public void validate() {

        final String query = "call CSVWRITE ( 'JDBC_OP', 'SELECT * FROM " + opTableName + " ', null, null,   '' ) ;";
        Connection conn = null;

        if (executionMode == ExecutionMode.EMBEDDED) {
            conn = jdbcTestService.getConnection();
            validateDBOutput(conn, "H2", query, EXPECTED_CSV_DATA_SET);
        } else {
            conn = getClusterConnection();
            try {
                validateClusterDBOutput(conn, opTableName, EXPECTED_CSV_DATA_SET);
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }

    }

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {
        if (cleanUp) {
            jdbcTestService.cleanUp();
            super.cleanUp();
        }
    }

    /**
     * Initialize output configs.
     */
    private void initializeOutputConfigs() {
        outputConfiguration = new HashMap<String, String>();
        outputConfiguration.put("uri", uri);
        outputConfiguration.put("driver", driver);
        outputConfiguration.put("user", userName);
        outputConfiguration.put("password", password);
        outputConfiguration.put("table.name", opTableName);
        outputConfiguration.put("name", "Jdbc_op_" + random.nextInt(9999));
    }

    /**
     * Initialize input configs.
     */
    private void initializeInputConfigs() {
        inputConfiguration = new HashMap<String, String>();
        inputConfiguration.put("name", "Jdbc_ip_" + random.nextInt(9999));
        inputConfiguration.put("uri", uri);
        inputConfiguration.put("driver", driver);
        inputConfiguration.put("user", userName);
        inputConfiguration.put("password", password);
        inputConfiguration.put("table.name", ipTableName);
    }

    /**
     * Gets the cluster connection.
     *
     * @return the cluster connection
     */
    private Connection getClusterConnection() {
        Connection connection = null;
        try {
            Class.forName(driver).newInstance();
        } catch (final Exception ex) {
            LOGGER.error("Check classpath. Cannot load db driver: " + driver);
            Assert.fail(ex.getMessage());
        }

        final String connectionUrl = uri.replace(IOURIS.JDBC.getUri(), "");

        try {
            connection = DriverManager.getConnection(connectionUrl, userName, password);
        } catch (final SQLException e) {
            LOGGER.error("Driver loaded, but cannot connect to db: " + connectionUrl);
            Assert.fail(e.getMessage());
        }

        return connection;
    }
}
