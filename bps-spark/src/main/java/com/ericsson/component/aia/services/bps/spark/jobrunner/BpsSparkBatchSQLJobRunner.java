package com.ericsson.component.aia.services.bps.spark.jobrunner;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.spark.jobrunner.common.SparkSessionHelper;

/**
 * SparkBatchJobRunner class is a one of the implementation for Step interface. This handler is used when the user wants to use Spark batch (With Hive
 * Context).
 */
public class BpsSparkBatchSQLJobRunner extends BpsSparkBatchJobRunner {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsSparkBatchSQLJobRunner.class);

    private static final long serialVersionUID = -250957532078855504L;

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_BATCH_SQL.getUri();
    }

    /**
     * Execute method runs Step of a pipeline job.
     */
    @Override
    public void executeJob() {
        LOG.trace("Entering the execute method");
        final String sqlQuery = getStepProperties().getProperty("sql");
        final Dataset sql = SparkSessionHelper.getSession().sqlContext().sql(sqlQuery);
        getOutGoing().write(sql);
        getOutGoing().cleanUp();
        LOG.trace("Exiting the execute method");
    }
}
