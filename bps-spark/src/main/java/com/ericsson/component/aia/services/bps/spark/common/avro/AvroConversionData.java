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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * DTO class used by SparkRddToAvro to cache conversion data on spark worker. Temporary class which can be removed along with SparkRddToAvro class.
 */
public class AvroConversionData {

    private final Schema avroSchema;
    private final Map<String, Schema> avroSchemaFields;
    private final List<String> arrayKeys;
    private final Long schemaId;

    /**
     *
     * @param schemaId
     *            The ID of the schema being converted.
     * @param avroSchema
     *            The Schema being converted.
     */
    AvroConversionData(final Long schemaId, final Schema avroSchema) {
        this.schemaId = schemaId;
        this.avroSchema = avroSchema;
        this.arrayKeys = getAvroArrayKeys(avroSchema);
        this.avroSchemaFields = getAvroFields(avroSchema);
    }

    public Map<String, Schema> getAvroSchemaFields() {
        return avroSchemaFields;
    }

    public List<String> getArrayKeys() {
        return arrayKeys;
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }

    public Long getSchemaId() {
        return schemaId;
    }

    private List<String> getAvroArrayKeys(final Schema schema) {
        final List<String> arrayKeys = new ArrayList<>();
        for (final Field field : schema.getFields()) {
            if (field.schema().getType() == Type.ARRAY) {
                arrayKeys.add(field.name());
            }
        }
        return arrayKeys;
    }

    private Map<String, Schema> getAvroFields(final Schema avroSchema) {
        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        for (final Field field : avroSchema.getFields()) {
            avroSchemaFields.put(field.name(), field.schema());
        }
        return avroSchemaFields;
    }
}
