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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;

/**
 * The <code>BpsSparkJdbcDataSourceService</code> is responsible for reading data from jdbc system and return respective {@link Dataset } .<br>
 *
 * The <code>BpsSparkJdbcDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, Dataset&gt;</code> which is specific to
 * HiveContext and Dataset. <br>
 * <br>
 */
public class BpsSparkJdbcDataSourceService extends AbstractBpsDataSourceService<SQLContext, Dataset> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkJdbcDataSourceService.class);

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }

    /**
     * Gets the Dataset based on the provided configurations in flow xml.
     *
     * @return the data stream
     */
    @Override
    public Dataset getDataStream() {
        LOGGER.trace("Entering the getJDBCTableContexts method ");
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        final String tableName = properties.getProperty(Constants.TABLE);
        final Dataset jdbc = context.read().jdbc(decode.getContext(), tableName, properties);
        jdbc.createOrReplaceTempView(tableName);
        LOGGER.trace("Returning the getJDBCTableContexts method ");
        return jdbc;
    }
}
