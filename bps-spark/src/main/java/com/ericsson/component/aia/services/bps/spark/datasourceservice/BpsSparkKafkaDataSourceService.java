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
import org.apache.spark.streaming.api.java.JavaDStream;

import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.spark.jobrunner.common.SparkSessionHelper;

/**
 * The <code>BpsSparkKafkaDataSourceService</code> is responsible for streaming data from Kafka system and return respective {@link Dataset } .<br>
 *
 * The <code>BpsSparkKafkaDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, &lt;String, GenericRecord&gt&gt;</code> which
 * is specific to HiveContext & GenericRecords. <br>
 * <br>
 */
public class BpsSparkKafkaDataSourceService extends AbstractBpsDataSourceService<SQLContext, JavaDStream<?>> {

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.KAFKA.getUri();
    }

    /**
     * Gets a JavaPair InputDStream based schema criteria.
     *
     * @param <K>
     * @param <V>
     *
     * @param <V>
     *
     * @return the data stream
     * @throws ClassNotFoundException
     */

    @Override
    public JavaDStream<?> getDataStream() {
        try {
            return SparkSessionHelper.createKafkaConnection(properties);
        } catch (final ClassNotFoundException e) {
            throw new BpsRuntimeException(e);
        }
    }
}
