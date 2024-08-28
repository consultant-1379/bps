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
package com.ericsson.component.aia.services.bps.spark.datasinkservice;

import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_ADDRESS_PARAMETER;
import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER;
import static com.ericsson.component.aia.services.bps.core.common.Constants.EVENT_TYPE;
import static com.ericsson.component.aia.services.bps.spark.common.avro.SparkRddToAvro.transformRddToAvroRdd;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.aia.common.datarouting.api.DataSink;
import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.model.registry.impl.SchemaRegistryClientFactory;
import com.ericsson.component.aia.model.registry.utils.Utils;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.builder.DataRoutingOutputBuilder;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.google.common.collect.Lists;

/**
 * The <code>BpsSparkDataRoutingSink</code> class is responsible for writing {@link Dataset } to a data router.<br>
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkDataRoutingSink<C> extends BpsAbstractDataSink<C, Dataset<Row>> implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -4916428491473581453L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkKafkaDataSink.class);

    /** The event type. */
    private String eventType;

    private BpsDataSinkConfiguration bpsDataSinkConfiguration;

    /** The schema registry url. */
    private String schemaRegistryUrl;

    /** The schema registry cache. */
    private int schemaRegistryCache;

    private transient SchemaRegistryClient schemaRegistryClient;

    private transient RegisteredSchema lookup;

    /**
     * Configured instance of {@link BpsSparkDataRoutingSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the configuration associated with underlying output sink.
     */
    @Override
    public void configureDataSink(final C context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        this.bpsDataSinkConfiguration = bpsDataSinkConfiguration;
        eventType = properties.getProperty(EVENT_TYPE);
        schemaRegistryUrl = Utils.getSchemaRegistryUrlProperty(properties);
        schemaRegistryCache = Utils.getSchemaRegistryCacheSizeProperty(properties);
        lookup = getEventSchema();
        LOGGER.trace("Initiating configureDataSink for {}. ", dataSinkContextName);
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        LOGGER.trace(String.format("Cleaning resources allocated for %s ", dataSinkContextName));
        strategy = null;
        LOGGER.trace(String.format("Cleaned resources allocated for %s ", dataSinkContextName));
    }

    /**
     * Gets the service context name.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.DATA_ROUTING.getUri();
    }

    /**
     * Send messages to Data Router.
     *
     * @param dataset
     *            the data stream
     */
    @Override
    public void write(final Dataset<Row> dataset) {
        LOGGER.info("BpsSparkDataRoutingSink [Context=" + getDataSinkContextName() + "] got Dataframe with [Rows=" + dataset.count() + "]");
        try {
            final Row first = dataset.head();
            final String[] fieldNames = first.schema().fieldNames();
            final JavaRDD<Row> rdd = dataset.javaRDD();
            final JavaRDD<GenericRecord> avroRdd = transformRddToAvroRdd(rdd, schemaRegistryUrl, schemaRegistryCache, fieldNames, lookup);

            avroRdd.foreachPartition(new VoidFunction<Iterator<GenericRecord>>() {
                private static final long serialVersionUID = 1L;
                private transient DataSink<GenericRecord> sink;

                @Override
                public void call(final Iterator<GenericRecord> it) {
                    getDataSinkInstance().sendMessage(Lists.newArrayList(it));
                }

                private DataSink<GenericRecord> getDataSinkInstance() {
                    if (sink == null) {
                        sink = new DataRoutingOutputBuilder<GenericRecord>().addOutputProperties(properties)
                                .addSinks(bpsDataSinkConfiguration.getSinks()).build();
                    }
                    return sink;
                }

                {
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            getDataSinkInstance().close();
                        }
                    });
                }
            });
        } catch (GenericRecordConversionException | SchemaRetrievalException e) {
            LOGGER.error("Exception occured when rating BpsSparkKafkaDataSink [Context=" + getDataSinkContextName()
                    + "] write operation completed successfuly.", e);
            e.printStackTrace();
        }
        LOGGER.info("BpsSparkDataRoutingSink [Context=" + getDataSinkContextName() + "] write operation completed successfuly.");
    }

    private RegisteredSchema getEventSchema() {
        final Properties properties = new Properties();
        properties.setProperty(SCHEMA_REGISTRY_ADDRESS_PARAMETER, schemaRegistryUrl);
        properties.setProperty(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER, String.valueOf(schemaRegistryCache));
        schemaRegistryClient = SchemaRegistryClientFactory.newSchemaRegistryClientInstance(properties);

        try {
            return schemaRegistryClient.lookup(eventType);
        } catch (final SchemaRetrievalException e) {
            throw new IllegalArgumentException("Unable to retrieve event type:: " + eventType + " from schema registry.", e);
        }
    }
}