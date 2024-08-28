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
package com.ericsson.component.aia.services.bps.spark.common.avro;

import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_ADDRESS_PARAMETER;
import static com.ericsson.component.aia.model.registry.utils.Constants.SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.ericsson.component.aia.common.avro.GenericRecordWrapper;
import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.model.registry.impl.SchemaRegistryClientFactory;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;

/**
 * The Utility class <code>SparkRddToAvro</code> provides utility methods to transform RDD to avro etc.
 *
 */
public final class SparkRddToAvro implements Serializable {

    /**
     * Please note that the class Schema is not serializable and so must be initialized on each worker. This field will be initialized once by each
     * spark worker and will be maintained by each worker. This solution is only temporary and will not work if workers are required to handle
     * multiple schemas.
     */
    private static Map<Long, AvroConversionData> schemaIdToConversionData = new ConcurrentHashMap<>();
    private static SchemaRegistryClient schemaRegistryClient;
    private static final long serialVersionUID = 1L;

    private SparkRddToAvro() {
    }

    /**
     * Method will transform JavaRDD<Row> to JavaRDD<GenericRecord>.
     *
     * @param rdd
     *            representing single events.
     * @param schemaRegistryUrl
     *            the schema registry URL.
     * @param schemaRegistryCache
     *            the schema registry cache.
     * @param fieldNames
     *            The names of the columns in the row.
     * @param schema
     *            The ID of the Event Schema.
     *
     * @return input RDD represented as a Generic records.
     *
     * @throws GenericRecordConversionException
     *             when conversion fails
     * @throws SchemaRetrievalException
     *             the schema retrieval exception
     */
    public static JavaRDD<GenericRecord> transformRddToAvroRdd(final JavaRDD<Row> rdd, final String schemaRegistryUrl, final int schemaRegistryCache,
                                                               final String[] fieldNames, final RegisteredSchema schema)
            throws GenericRecordConversionException, SchemaRetrievalException {

        final Long schemaId = schema.getSchemaId();

        return rdd.map(new Function<Row, GenericRecord>() {

            private static final long serialVersionUID = 1L;

            @Override
            public GenericRecord call(final Row sqlRow) throws GenericRecordConversionException, SchemaRetrievalException {
                AvroConversionData avroConvesionData = schemaIdToConversionData.get(schemaId);

                if (avroConvesionData == null) {
                    avroConvesionData = initializeConversionData(schemaRegistryUrl, schemaRegistryCache, schemaId);
                }

                return transformRowToAvroRecord(sqlRow, fieldNames, avroConvesionData);
            }

            private AvroConversionData initializeConversionData(final String schemaRegistryUrl, final int schemaRegistryCache, final Long schemaId)
                    throws SchemaRetrievalException {

                schemaRegistryClient = getSchemaRegistryClient(schemaRegistryUrl, schemaRegistryCache);
                final Schema avroSchema = schemaRegistryClient.lookup(schemaId);
                final AvroConversionData avroConvesionData = new AvroConversionData(schemaId, avroSchema);
                schemaIdToConversionData.put(schemaId, avroConvesionData);
                return avroConvesionData;
            }

            private SchemaRegistryClient getSchemaRegistryClient(final String schemaRegistryUrl, final int schemaRegistryCache) {
                if (schemaRegistryClient == null) {
                    final Properties properties = new Properties();
                    properties.setProperty(SCHEMA_REGISTRY_ADDRESS_PARAMETER, schemaRegistryUrl);
                    properties.setProperty(SCHEMA_REGISTRY_CACHE_MAX_SIZE_PARAMETER, String.valueOf(schemaRegistryCache));
                    schemaRegistryClient = SchemaRegistryClientFactory.newSchemaRegistryClientInstance(properties);
                }
                return schemaRegistryClient;
            }

        });
    }

    /**
     * This method generate GenericRecord object from row object by mapping row header with AVRO fields.
     *
     * @param row
     *            Individual row of JavaRDD or Dataframe
     * @param rddSchemaFields
     *            Map of row columns.
     * @param avroConvesionData
     *            A data transfer object which contains all of the data necessary info about the AVRO schema.
     *
     * @return GenericRecord if at least one field is matched between AVRO schema and row fields else throw exception.
     * @throws GenericRecordConversionException
     *             if non of the field is common between AVRO schema and row fields.
     * @throws SchemaRetrievalException
     *             the schema retrieval exception
     */
    public static GenericRecord transformRowToAvroRecord(final Row row, final String[] rddSchemaFields, final AvroConversionData avroConvesionData)
            throws GenericRecordConversionException, SchemaRetrievalException {

        final GenericRecordWrapper wrapper = new GenericRecordWrapper(avroConvesionData.getSchemaId(), avroConvesionData.getAvroSchema());
        addDefaultArrayValues(avroConvesionData.getArrayKeys(), wrapper);

        boolean isAnyValueMatched = false;
        for (int index = 0; index < row.size(); index++) {
            final Schema schema = avroConvesionData.getAvroSchemaFields().get(rddSchemaFields[index]);

            if (schema != null) {
                final Object object = row.get(index);
                if ((object != null) && !object.toString().equalsIgnoreCase("null")) {
                    final Object typeObject = SparkTypeUtility.getTypeObject(schema, object);
                    if (typeObject != null) {
                        wrapper.put(rddSchemaFields[index], typeObject);
                        isAnyValueMatched = true;
                    }
                }
            }
        }
        if (!isAnyValueMatched) {
            final StringBuilder buildErrorMessage = new StringBuilder();
            buildErrorMessage.append("Unable to generate genericrecord as unable to mapped columns -->");
            buildErrorMessage.append(" { Avro Schema conatins ").append(avroConvesionData.getAvroSchemaFields().keySet()).append(" whereas ");
            buildErrorMessage.append("Dataset conatins ").append(Arrays.asList(rddSchemaFields)).append("}");
            throw new GenericRecordConversionException(buildErrorMessage.toString());
        }
        return wrapper;
    }

    private static void addDefaultArrayValues(final List<String> arrayKeys, final GenericRecordWrapper wrapper) {
        for (final String arrayKey : arrayKeys) {
            wrapper.put(arrayKey, Collections.EMPTY_LIST);
        }
    }

}
