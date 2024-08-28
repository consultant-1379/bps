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

import static com.ericsson.component.aia.services.bps.spark.common.AlluxioConfigEnum.BEAN_CLASS;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.spark.utils.SparkUtil;

/**
 * The <code>BpsSparkAlluxioDataSourceService</code> is responsible for reading data from Alluxio system and return respective {@link Dataset }.
 *
 * <br>
 *
 * The <code>BpsSparkAlluxioDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, Dataset&gt;</code> which is specific to
 * HiveContext and Dataset. <br>
 * <br>
 */
public class BpsSparkAlluxioDataSourceService extends AbstractBpsDataSourceService<SQLContext, Dataset> {

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.ALLUXIO.getUri();
    }

    /**
     * Gets the Dataset based on the provided configurations in flow xml.
     *
     * @return the data stream
     */
    @Override
    public Dataset getDataStream() {
        if (properties.containsKey(BEAN_CLASS.getConfiguration())) {
            try {
                return SparkUtil.getDfFromObjectFile(context, properties);
            } catch (final ClassNotFoundException e) {
                throw new BpsRuntimeException(e);
            }
        } else {
            return SparkUtil.getDataFrame(context, properties);
        }
    }
}
