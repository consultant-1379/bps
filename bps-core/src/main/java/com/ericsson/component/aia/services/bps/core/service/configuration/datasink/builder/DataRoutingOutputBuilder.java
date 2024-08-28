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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasink.builder;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.ericsson.aia.common.datarouting.api.DataSink;
import com.ericsson.aia.common.datarouting.api.builder.AcceptedRecordBuilder;
import com.ericsson.aia.common.datarouting.api.builder.DataRouterBuilder;
import com.ericsson.aia.common.datarouting.api.builder.DataRouterSinkBuilder;
import com.ericsson.aia.common.datarouting.api.builder.OutputDataRouterBuilder;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.ByFunctionType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.ByNameType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.ByRegexType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.BySchemaType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FilterType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.PartitionStrategyType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.PropertyType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.RecordType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;

/**
 * This class is used as a builder to create a Data-Routing DataSink. It is an intermediate between the flow in the XML and the Data Routing API.
 *
 * @param <V>
 *            The type of the record which will be output.
 */
public class DataRoutingOutputBuilder<V> {

    private final OutputDataRouterBuilder<V> outputDataRouterBuilder = DataRouterBuilder.outputRouter();

    /**
     * Used to set the Master properties which will be added to every sink under the output.
     *
     * @param properties
     *            The master properties for the output which every sink should have.
     * @return The builder.
     */
    public DataRoutingOutputBuilder<V> addOutputProperties(final Properties properties) {
        return this;
    }

    /**
     * Adds a group of sinks which should be added to the data routing output.
     *
     * @param sinks
     *            The sinks which this output should contain.
     * @return The builder.
     */
    public DataRoutingOutputBuilder<V> addSinks(final Collection<SinkType> sinks) {
        for (final SinkType sink : sinks) {
            final DataRouterSinkBuilder<V> dataRouterSinkBuilder = outputDataRouterBuilder.addSink(sink.getUri());
            addPartitionStrategyToBuilder(dataRouterSinkBuilder, sink.getDataRouting().getPartitionStrategy());
            addFilterToBuilder(dataRouterSinkBuilder, sink.getFilter());
            dataRouterSinkBuilder.setProperties(convertToProperties(sink.getProperty()));
        }
        return this;
    }

    /**
     * Builds the data routing sink from the set sinks.
     *
     * @return The Data routing sink to be used by the output.
     */
    public DataSink<V> build() {
        return outputDataRouterBuilder.build();
    }

    private void addFilterToBuilder(final DataRouterSinkBuilder<?> dataRouterSinkBuilder, final FilterType filter) {
        if (filter != null) {
            for (final RecordType acceptedRecords : filter.getRecords().getRecord()) {
                final AcceptedRecordBuilder<?> acceptedRecordBuilder = dataRouterSinkBuilder.filter().acceptRecord();

                for (final ByFunctionType byFunction : acceptedRecords.getByFunction()) {
                    acceptedRecordBuilder.function(byFunction.getValue());
                }

                for (final ByRegexType byRegex : acceptedRecords.getByRegex()) {
                    acceptedRecordBuilder.regex(byRegex.getAttribute(), byRegex.getValue());
                }

                for (final BySchemaType bySchema : acceptedRecords.getBySchema()) {
                    acceptedRecordBuilder.schema(bySchema.getValue());
                }

                for (final ByNameType byName : acceptedRecords.getByName()) {
                    acceptedRecordBuilder.name(byName.getValue());
                }
            }
        }
    }

    private void addPartitionStrategyToBuilder(final DataRouterSinkBuilder<?> dataRouterSinkBuilder, final PartitionStrategyType partitionStrategy) {
        if (partitionStrategy.getByKey() != null) {
            dataRouterSinkBuilder.partitionStrategy().applyKey(partitionStrategy.getByKey().getValue());

        } else if (partitionStrategy.getRoundRobin() != null) {
            final String numberOfPartitions = partitionStrategy.getRoundRobin().getValue();

            if ((numberOfPartitions != null) && !numberOfPartitions.isEmpty()) {
                dataRouterSinkBuilder.partitionStrategy().applyRoundRobin(Integer.parseInt(numberOfPartitions));
            } else {
                dataRouterSinkBuilder.partitionStrategy().applyRoundRobin();
            }

        } else if (partitionStrategy.getByFunction() != null) {
            dataRouterSinkBuilder.partitionStrategy().applyFunction(partitionStrategy.getByFunction().getValue());
        }
    }

    private Properties convertToProperties(final List<PropertyType> propertiesFromFlow) {
        final Properties properties = new Properties();
        for (final PropertyType property : propertiesFromFlow) {
            properties.put(property.getName(), property.getValue());
        }
        return properties;
    }
}
