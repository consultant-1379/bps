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

import static com.ericsson.component.aia.services.bps.core.common.enums.KafkaDataFormats.AVRO;
import static com.ericsson.component.aia.services.bps.core.common.enums.KafkaDataFormats.JSON;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.services.bps.core.common.Constants;
import com.ericsson.component.aia.services.bps.core.common.URIDefinition;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.services.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.services.bps.spark.kafka.AvroDatasetWriter;
import com.ericsson.component.aia.services.bps.spark.kafka.JsonDatasetWriter;

/**
 * The <code>BpsSparkFileDataSink</code> class is responsible for writing {@link Dataset } to a Kafka topic.<br>
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
@SuppressWarnings("rawtypes")
public class BpsSparkKafkaDataSink<C> extends BpsAbstractDataSink<C, Dataset> implements Serializable {

    /**
     * Optional attribute for custom writer which implements {@link com.ericsson.component.aia.services.bps.spark.datasinkservice.DataWriter}
     * interface. User can supply <B>datawriter.class</B> attribute in part of sink configuration with fully qualified class name. Note: Provided
     * implementation should have <b> NO ARGUMENT CONSTRUCTOR WITH PUBLIC SCOPE </B>.
     */
    private static final String DATAWRITER_CLASS = "datawriter.class";

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -4916428491473581453L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkKafkaDataSink.class);

    private DataWriter<Dataset> dataWriter;

    /** The format. */
    private String format;

    /** The topic. */
    private String topic;

    /**
     * Configured instance of {@link BpsSparkKafkaDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param bpsDataSinkConfiguration
     *            the bps data sink configuration
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configureDataSink(final C context, final BpsDataSinkConfiguration bpsDataSinkConfiguration) {
        super.configureDataSink(context, bpsDataSinkConfiguration);
        LOGGER.trace("Initiating configureDataSink for {}. ", getDataSinkContextName());

        final URIDefinition<IOURIS> decode = IOURIS.decode(properties);
        format = decode.getParams().getProperty(Constants.FORMAT);
        topic = decode.getContext();

        final String customWriter = properties.getProperty(DATAWRITER_CLASS);
        if (customWriter != null && !customWriter.trim().isEmpty()) {
            try {
                final Object customDataWriterClzz = Class.forName(customWriter.trim()).newInstance();
                if (customDataWriterClzz instanceof DataWriter) {
                    dataWriter = (DataWriter<Dataset>) customDataWriterClzz;
                    dataWriter.setSinkProperties(properties);
                } else {
                    throw new IllegalArgumentException(String.format("Provided custom datawrite %s for Sink %s with context %s are not instanceof %s",
                            customWriter, this.getClass().getName(), getDataSinkContextName(), DataWriter.class.getName()));
                }
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException(String.format("Unable to initialise custom datawrite %s for Sink %s with context %s", customWriter,
                        this.getClass().getName(), getDataSinkContextName()), e);
            }
            LOGGER.info("Sink {}  with context {} initialised with custom datawriter {}", this.getClass().getName(), getDataSinkContextName(),
                    customWriter);

        } else if (AVRO.getDataFormat().equalsIgnoreCase(format)) {
            dataWriter = new AvroDatasetWriter(properties, topic);

        } else if (JSON.getDataFormat().equalsIgnoreCase(format)) {
            dataWriter = new JsonDatasetWriter(properties, topic);
        } else {
            throw new IllegalArgumentException("Currenlty Kafka supports avro and json format and custom datawriter only");
        }

        LOGGER.info("Configuring {} for the output  {} for Kafka topic {}", this.getClass().getName(), dataSinkContextName, decode.getContext());

    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {
        LOGGER.trace("Cleaning resources allocated for {} ", getDataSinkContextName());
        strategy = null;
        if (dataWriter != null) {
            dataWriter.cleanUp();
        }
        LOGGER.trace("Cleaned resources allocated for {} ", getDataSinkContextName());
    }

    /**
     * Gets the service context name.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.KAFKA.getUri();
    }

    /**
     * Send messages to Kafka.
     *
     * @param dataFrame
     *            the data stream
     */
    @Override
    public void write(final Dataset dataFrame) {
        LOGGER.trace("BpsSparkKafkaDataSink [Context=" + getDataSinkContextName() + "] got Dataframe with [Rows=" + dataFrame.count() + "]");
        dataWriter.write(dataFrame);
        LOGGER.trace("BpsSparkKafkaDataSink [Context=" + getDataSinkContextName() + "] write operation completed successfuly.");
    }
}
