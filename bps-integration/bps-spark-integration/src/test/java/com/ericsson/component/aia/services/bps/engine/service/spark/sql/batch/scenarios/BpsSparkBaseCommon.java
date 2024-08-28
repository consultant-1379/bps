package com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.scenarios;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.services.bps.test.common.TestConstants.FLOW;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.importer.ZipImporter;
import org.junit.Assert;

import com.ericsson.component.aia.services.bps.core.common.ExecutionMode;
import com.ericsson.component.aia.services.bps.engine.service.BPSPipeLineExecuter;
import com.ericsson.component.aia.services.bps.engine.service.spark.sql.batch.jar.util.SparkBpsJarAssembler;
import com.ericsson.component.aia.services.bps.test.cluster.util.HttpUrlRequestPostClient;
import com.ericsson.component.aia.services.bps.test.cluster.util.SubmitJobStatus;
import com.ericsson.component.aia.services.bps.test.common.util.FlowXmlGenerator;
import com.ericsson.component.aia.services.bps.test.pipeExecutor.BpsScenariosBaseTest;
import com.ericsson.component.aia.services.bps.test.pipeExecutor.beans.TestScenarioDetailsBean;

public class BpsSparkBaseCommon extends BpsScenariosBaseTest {

    /**
     * The Constant LOGGER.
     */
    private static final Logger LOGGER = Logger.getLogger(BpsSparkBaseCommon.class);

    private String restSubmitRequest = "sparkJsonRequest";

    private final String SUBMISSION_STATUS = "v1/submissions/status/";

    private final String SUBMISSION_CREATE = "v1/submissions/create";

    /**
     * Instantiates a new test BPS pipe line executor cases.
     */
    public BpsSparkBaseCommon(final TestScenarioDetailsBean testScenarioBean) {
        super(testScenarioBean);

    }

    /**
     * Executes test case scenario pipeline based on flow xml.,
     *
     * @param context
     *            the context
     * @param target_op
     *            the target op
     */
    public void executePipeLine(final Map<String, List<Map<String, String>>> context, final Path target_op, final ExecutionMode executionMode) {

        LOGGER.debug("Creating Flow xml for the test scenario");
        FlowXmlGenerator.createFlowXml(target_op, context);
        LOGGER.debug("Created Flow xml for the test scenario");
        final String flowXML = target_op.toFile().toString() + SEPARATOR + FLOW;
        LOGGER.debug("Started running pipeline");
        if (executionMode == ExecutionMode.EMBEDDED) {
            try{
                BPSPipeLineExecuter.main(new String[] { flowXML });
            }
            catch(Exception exp){
                LOGGER.error("Exception occurred while testing Pipe-line application, reason \n", exp);
            }
        } else {
            try {

                final BpsCommonScenario bpsCommonScenario = new SparkBpsJarAssembler();
                final ZipImporter bpsdependecies = bpsCommonScenario.createJar();
                final String hdfsFilePath = copyFilesOnCLuster(flowXML);
                final String jarPath = target_op + "/myTests.jar";
                bpsdependecies.as(ZipExporter.class).exportTo(new File(jarPath), true);
                final String hdfsJarPath = copyFilesOnCLuster(jarPath);
                final Map<String, String> attributesMap = getAttributesMap(hdfsFilePath, hdfsJarPath);
                final String urlPath = attributesMap.get("urlPath");
                LOGGER.debug("Started running pipeline");
                final String submissionId = HttpUrlRequestPostClient.CreateSubmissionResponse(urlPath, attributesMap, restSubmitRequest);
                LOGGER.info("Spark Job submissionId = " + submissionId);
                final String sparkUrl = attributesMap.get("sparkUrl");
                final String submitUrl = sparkUrl + SUBMISSION_STATUS + submissionId;
                final String driverState = SubmitJobStatus.getJobDriverState(submitUrl);

                if (driverState.equalsIgnoreCase("ERROR")) {
                    Assert.fail("Submitted Job Failed to execute because of the error. Please check Spark Worker logs.");
                }
                LOGGER.debug("Started running pipeline");
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }

    }

    protected Map<String, String> getAttributesMap(final String hdfsFilePath, final String hdfsJarPath) {
        final String sparkUrl = properties.getProperty("spark.url");
        final String hdfsUrl = properties.getProperty("hdfsURL");
        final String clusterUrl = properties.getProperty("sparkClusterUrl");
        final String urlPath = sparkUrl + SUBMISSION_CREATE;
        final Map<String, String> argumentMap = new HashMap<>();
        argumentMap.put("jobArgument", hdfsUrl + hdfsFilePath);
        argumentMap.put("applicationJar", hdfsUrl + hdfsJarPath);
        argumentMap.put("mainClass", BPSPipeLineExecuter.class.getCanonicalName());
        argumentMap.put("jobName", "SubmitJobStatus");
        argumentMap.put("sparkClusterUrl", clusterUrl);
        argumentMap.put("sparkUrl", sparkUrl);
        argumentMap.put("urlPath", urlPath);
        return argumentMap;
    }
}
