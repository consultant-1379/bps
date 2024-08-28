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
package com.ericsson.component.aia.services.bps.spark.datasourceservice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;

/**
 * The <code>BpsSparkHdfsDataSourceService</code> is responsible for reading data from hdfs system and return respective {@link Dataset } .<br>
 *
 * The <code>BpsSparkHdfsDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, Dataset&gt;</code> which is specific to
 * HiveContext and Dataset. <br>
 * <br>
 */
public class BpsSparkHdfsDataSourceService extends AbstractBpsDataSourceService<SQLContext, Dataset> {

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.HDFS.getUri();
    }

    /**
     * Gets the Dataset based on the provided configurations in flow xml.
     *
     * @return the data stream
     */
    @Override
    public Dataset getDataStream() {
        return SparkUtil.getDataFrame(context, properties);
    }
}
