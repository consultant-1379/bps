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

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * AvroToSqlConverterUtil Class contains method that are used to convert Avro schemas to SparkSQL schemas.
 */
@SuppressWarnings({ "PMD.CyclomaticComplexity", "PMD.StdCyclomaticComplexity" })
public class AvroToSqlConverterUtil implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 7500209240887964117L;

    private AvroToSqlConverterUtil() {

    }

    /**
     *
     * This function takes an avro schema and returns a sql schema.
     *
     *
     * @param schema
     *            the field
     * @return the data type
     */

    public static DataType toSqlType(final Schema schema) {

        switch (schema.getType()) {
            case INT:
                return DataTypes.IntegerType;
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case BYTES:
                return DataTypes.BinaryType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case FLOAT:
                return DataTypes.FloatType;
            case LONG:
                return DataTypes.LongType;
            case FIXED:
                return DataTypes.BinaryType;
            case ENUM:
                return DataTypes.StringType;
            case ARRAY:
                final DataType arrayElementDataType = toSqlType(schema.getElementType());
                return DataTypes.createArrayType(arrayElementDataType);
            case RECORD:
                final List<Field> recordFields = schema.getFields();
                final List<StructField> structFields = new LinkedList<>();
                for (final Field recordField : recordFields) {
                    structFields.add(DataTypes.createStructField(recordField.name(), toSqlType(recordField.schema()), true));
                }
                return DataTypes.createStructType(structFields);
            case MAP:
                return DataTypes.createMapType(DataTypes.StringType, toSqlType(schema.getValueType()));

            case UNION:
                return convertAvroToSqlSchema(schema);

            default:
                return null;
        }
    }

    private static DataType convertAvroToSqlSchema(final Schema schema) {
        final List<Schema> types = schema.getTypes();
        if (types != null) {
            if (types.size() == 1) {
                return toSqlType(types.get(0));

            } else {
                if (types.contains(Type.LONG) && types.contains(Type.INT)) {
                    return DataTypes.LongType;

                } else if (types.contains(Type.FLOAT) && types.contains(Type.DOUBLE)) {
                    return DataTypes.DoubleType;

                } else {
                    final int index = 0;
                    final StructField[] fields = new StructField[types.size()];
                    for (final Schema type : types) {
                        DataTypes.createStructField("member" + index, toSqlType(type), true);
                    }
                    return DataTypes.createStructType(fields);
                }
            }
        }
        return null;
    }

    /**
     * Returns a converter function to convert row in avro format to GenericRow of catalyst.
     *
     * This method will be removed once we upgrade Spark to 2.0 vesrsion and use Spark-avro 3.X version
     *
     * @param record
     *            the record
     * @param structField
     *            the struct field
     * @return the object
     *
     *         @deprecated("Use Spark-avro incase of Spark2.0", "3.0")
     */

    public static Object createConverterToSQL(final GenericRecord record, final StructField structField) {

        final Object recordValue = record.get(structField.name());

        if (null == recordValue) {
            return null;
        }

        return converterToSQLObject(structField.dataType(), recordValue);
    }

    /**
     * Returns a converter function to convert row in avro format to GenericRow of catalyst.
     *
     * @param dataType
     *            the data type
     * @param recordValue
     *            the record value
     * @return the object
     */
    private static Object converterToSQLObject(final DataType dataType, final Object recordValue) {
        switch (dataType.toString()) {

            case "IntegerType":
                return Integer.parseInt(recordValue.toString());

            case "DoubleType":
                return Double.parseDouble(recordValue.toString());

            case "FloatType":
                return Float.parseFloat(recordValue.toString());

            case "LongType":
                return Long.parseLong(recordValue.toString());

            case "BooleanType":
                return Boolean.parseBoolean(recordValue.toString());

            case "ArrayType":
                return convertToArray(dataType, recordValue);

            case "MapType":
                return convertToMap(dataType, recordValue);

            case "StructField":
                return convertToStruct(dataType, recordValue);

            default:
                return recordValue.toString();
        }
    }

    private static Object convertToStruct(final DataType dataType, final Object recordValue) {
        final StructType structSchema = (StructType) dataType;
        final StructField[] structFields = structSchema.fields();
        final Object[] attributes = new Object[structFields.length];
        int index = 0;
        for (final StructField structField : structFields) {
            attributes[index] = createConverterToSQL((GenericRecord) recordValue, structField);
            index++;
        }
        return new GenericRow(attributes);
    }

    private static Object convertToMap(final DataType dataType, final Object recordValue) {
        final ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        final DataType mapValueType = ((org.apache.spark.sql.types.MapType) dataType).valueType();
        final Map<String, ? extends Object> recordMap = (Map<String, ?>) recordValue;
        for (final Entry<String, ?> entry : recordMap.entrySet()) {
            mapBuilder.put(entry.getKey().toString(), converterToSQLObject(mapValueType, entry.getValue()));
        }
        return mapBuilder.build();
    }

    private static Object convertToArray(final DataType dataType, final Object recordValue) {
        final DataType arrayElementDataType = ((org.apache.spark.sql.types.ArrayType) dataType).elementType();

        final ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
        for (final Object elementValue : (Iterable<?>) recordValue) {
            listBuilder.add(converterToSQLObject(arrayElementDataType, elementValue));
        }
        return listBuilder.build();
    }

}
