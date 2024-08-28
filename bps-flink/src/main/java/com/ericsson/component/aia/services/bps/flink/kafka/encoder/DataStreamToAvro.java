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
package com.ericsson.component.aia.services.bps.flink.kafka.encoder;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.ericsson.component.aia.common.avro.GenericRecordWrapper;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.services.bps.core.common.avro.Utility;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;

/**
 * The Utility class <code>DataStreamToAvro</code> provides utility methods to transform DataStream<T> to DataStream<GenericRecord>.
 */
public final class DataStreamToAvro implements Serializable {

    private static final long serialVersionUID = 1L;

    private DataStreamToAvro() {
    }

    /**
     * Method will transform DataStream<T> to DataStream<GenericRecord>.
     *
     * @param <T>
     *            Individual event type of DataStream
     * @param dataStream
     *            representing events.
     * @param registeredSchema
     *            the registered schema
     * @return DataStream<GenericRecord>.
     * @throws GenericRecordConversionException
     *             when conversion fails
     */
    public static <T> DataStream<GenericRecord> transformAvroDataStream(final DataStream<T> dataStream, final RegisteredSchema registeredSchema)
            throws GenericRecordConversionException {
        final LinkedHashMap<String, Type> avroSchemaFields = new LinkedHashMap<>();
        final Schema schema = registeredSchema.getSchema();
        final String schemaAsString = schema.toString();
        final List<Field> fields = schema.getFields();
        final Long identifier = registeredSchema.getSchemaId();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema().getType());
        }

        return dataStream.map(new MapFunction<T, GenericRecord>() {
            private static final long serialVersionUID = 1L;

            @Override
            public GenericRecord map(final T version1) throws GenericRecordConversionException {
                return transforEventToAvroRecord(version1, avroSchemaFields, identifier, schemaAsString);
            }
        });
    }

    /**
     * This method generate GenericRecord object from type object by mapping type fields with avro fields.
     *
     * @param <T>
     *            Individual event type of DataStream
     * @param type
     *            Individual event of DataStream
     * @param avroSchemaFields
     *            Map of avro schema field name as key and datatype as value.
     * @param schemaID
     *            avro schema id.
     * @param schemaString
     *            schema of the OUT type.
     * @return GenericRecordWrapper if at least one field is matched between avro schema and type fields else throw exception.
     * @throws GenericRecordConversionException
     *             if non of the field is common between avro schema and type fields.
     */
    private static <T> GenericRecord transforEventToAvroRecord(final T type, final Map<String, Type> avroSchemaFields, final long schemaID,
                                                               final String schemaString)
            throws GenericRecordConversionException {
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaString);
        final GenericRecordWrapper wrapper = new GenericRecordWrapper(schemaID, schema);
        boolean isAnyValueMatched = false;
        if (type instanceof Row) {
            convertRowRecords((Row) type, avroSchemaFields, wrapper);
        } else {
            final java.lang.reflect.Field[] typeFields = type.getClass().getDeclaredFields();
            for (int index = 0; index < typeFields.length; index++) {
                isAnyValueMatched = isAnyValueMatched(type, avroSchemaFields, wrapper, isAnyValueMatched, typeFields[index]);
            }
            if (!isAnyValueMatched) {
                final String[] typeFieldNames = new String[typeFields.length];
                for (int index = 0; index < typeFields.length; index++) {
                    typeFieldNames[index] = typeFields[index].getName();
                }
                final StringBuilder buildErrorMessage = new StringBuilder();
                buildErrorMessage.append("Unable to generate genericrecord as unable to mapped columns -->");
                buildErrorMessage.append(" { Avro Schema conatins ").append(avroSchemaFields.keySet()).append(" whereas ");
                buildErrorMessage.append("DataStream conatins ").append(Arrays.asList(typeFieldNames)).append("}");
                throw new GenericRecordConversionException(buildErrorMessage.toString());
            }
        }
        return wrapper;
    }

    private static <T> boolean isAnyValueMatched(final T type, final Map<String, Type> avroSchemaFields, final GenericRecordWrapper wrapper,
                                                 boolean isAnyValueMatched, final java.lang.reflect.Field typeField)
            throws GenericRecordConversionException {
        typeField.setAccessible(true);
        final String typeFieldName = typeField.getName();
        final Type fieldType = avroSchemaFields.get(typeFieldName);
        if (fieldType != null) {
            final Object object;
            try {
                object = typeField.get(type);
            } catch (IllegalArgumentException | IllegalAccessException cause) {
                throw new GenericRecordConversionException(cause.getMessage());
            }
            if (object != null && !object.toString().equalsIgnoreCase("null")) {
                final Object typeObject = Utility.getTypeObject(fieldType, object);
                if (typeObject != null) {
                    wrapper.put(typeFieldName, typeObject);
                    isAnyValueMatched = true;
                }
            }
        }
        return isAnyValueMatched;
    }

    private static <T> void convertRowRecords(final Row type, final Map<String, Type> avroSchemaFields, final GenericRecordWrapper wrapper) {
        final LinkedList<String> typeFieldNames = new LinkedList<>();
        for (final Map.Entry<String, Type> entry : avroSchemaFields.entrySet()) {
            typeFieldNames.add(entry.getKey());
        }
        for (int index = 0; index < avroSchemaFields.size(); index++) {
            final Object typeObject = Utility.getTypeObject(avroSchemaFields.get(typeFieldNames.get(index)), type.getField(index));
            if (typeObject != null) {
                wrapper.put(typeFieldNames.get(index), typeObject);
            }
        }
    }
}