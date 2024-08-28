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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.sql.Row;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * The <code>Utility</code> class contains common utility methods.
 */
@SuppressWarnings("PMD")
public class SparkTypeUtility {

    private SparkTypeUtility() {

    }

    /**
     * This method use to convert convert and/or get the value as per the type whenever possible. <br>
     *
     * @param schema
     *            : expected schema
     * @param object
     *            : object that needs to be validated/convert as per the expected type.
     * @return converted or real object as per the type.
     */
    public static Object getTypeObject(final Schema schema, final Object object) {
        final Type type = schema.getType();
        if (object != null) {
            if (object instanceof Number) {
                return convertFromPrimitive(object, type);

            } else if ((object instanceof String) || (object instanceof Character)) {
                return convertFromString(object, type);

            } else {
                return convertFromComplexDataType(schema, object, type);
            }
        }

        return object;

    }

    private static Object convertFromPrimitive(final Object object, final Type type) {
        if ((object instanceof Integer) || (object instanceof Long) || (object instanceof Byte)) {
            switch (type) {
                case INT:
                    if (object instanceof Integer) {
                        return object;
                    }
                    return Long.valueOf(object.toString()).intValue();
                case LONG:
                    if (object instanceof Long) {
                        return object;
                    }
                    return Long.valueOf(object.toString()).longValue();
                case FLOAT:
                    return Long.valueOf(object.toString()).floatValue();
                case DOUBLE:
                    return Long.valueOf(object.toString()).doubleValue();
                case STRING:
                    return String.valueOf(object);
                case BYTES:
                    return ByteBuffer.wrap(String.valueOf(object).getBytes());
                case BOOLEAN:
                    // for +ve number return true and for 0 and -ve return false
                    return Long.valueOf(object.toString()).intValue() > 0;
                default:
                    return object;
            }

        } else if ((object instanceof Double) || (object instanceof Float)) {

            switch (type) {
                case INT:
                    return Double.valueOf(object.toString()).intValue();
                case LONG:
                    return Double.valueOf(object.toString()).longValue();
                case FLOAT:
                    if (object instanceof Float) {
                        return object;
                    }
                    return Double.valueOf(object.toString()).floatValue();
                case DOUBLE:
                    if (object instanceof Double) {
                        return object;
                    }
                    return Double.valueOf(object.toString()).doubleValue();
                case STRING:
                    return String.valueOf(object);
                case BYTES:
                    return ByteBuffer.wrap(String.valueOf(object).getBytes());
                case BOOLEAN:
                    // for +ve number return true and for 0 and -ve return false
                    return Double.valueOf(object.toString()).intValue() > 0;
                default:
                    return object;
            }
        }
        return object;
    }

    private static Object convertFromComplexDataType(final Schema schema, final Object object, final Type type) {
        switch (type) {
            case ARRAY:
                return convertArray(schema, object);

            case MAP:
                return convertMap(schema, object);

            case RECORD:
                return convertRecord(schema, object);

            case UNION:
                return convertStructToUnion(schema, object);

            case BYTES:
                return ByteBuffer.wrap((byte[]) object);

            default:
                return object;
        }

    }

    private static Object convertFromString(final Object object, final Type type) {
        final String trim = object.toString().trim();
        switch (type) {
            case INT:
                if (NumberUtils.isNumber(trim)) {
                    return Double.valueOf(trim).intValue();
                }
                return null;
            case LONG:
                if (NumberUtils.isNumber(trim)) {
                    return Double.valueOf(trim).longValue();
                }
                return null;
            case FLOAT:
                if (NumberUtils.isNumber(trim)) {
                    return Double.valueOf(trim).floatValue();
                }
                return null;
            case DOUBLE:
                if (NumberUtils.isNumber(trim)) {
                    return Double.valueOf(trim).doubleValue();
                }
                return null;
            case STRING:
                if (object instanceof String) {
                    return object;
                }
                return String.valueOf(object);
            case BYTES:
                return ByteBuffer.wrap(String.valueOf(object).getBytes());
            case BOOLEAN:
                if (trim.matches("[0-9]*\\.?[0-9]*")) {
                    return Double.valueOf(trim) >= 1 ? true : false;
                } else if (trim.matches("(?i)(true|false)")) {
                    return Boolean.valueOf(trim);
                }
                return false;
            default:
                return object;
        }

    }

    private static Object convertArray(final Schema schema, final Object object) {
        final Iterable<?> recordValues = scala.collection.JavaConversions.asJavaCollection((scala.collection.Iterable<?>) object);
        final ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
        for (final Object elementValue : recordValues) {
            listBuilder.add(getTypeObject(schema.getElementType(), elementValue));
        }
        return listBuilder.build();
    }

    private static Object convertMap(final Schema schema, final Object object) {
        final ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
        final Schema mapValueSchema = schema.getValueType();

        final Map<String, ? extends Object> recordMap = scala.collection.JavaConversions.mapAsJavaMap((scala.collection.Map) object);
        for (final Entry<String, ?> entry : recordMap.entrySet()) {
            mapBuilder.put(entry.getKey().toString(), getTypeObject(mapValueSchema, entry.getValue()));
        }
        return mapBuilder.build();
    }

    private static Object convertRecord(final Schema schema, final Object object) {
        final Record record = new Record(schema);
        final Row row = (Row) object;
        final List<Field> fields = schema.getFields();
        for (int index = 0; index < row.size(); index++) {
            final Object rowElement = row.get(index);
            if ((rowElement != null) && !rowElement.toString().equalsIgnoreCase("null")) {
                record.put(index, SparkTypeUtility.getTypeObject(fields.get(index).schema(), rowElement));
            }
        }
        return record;
    }

    private static Object convertStructToUnion(final Schema schema, final Object object) {
        final List<Schema> types = schema.getTypes();
        if (types != null) {
            if (types.size() == 1) {
                return getTypeObject(types.get(0), object);

            } else {
                if (types.contains(Type.LONG) && types.contains(Type.INT)) {
                    return getTypeObject(Schema.create(Type.LONG), object);

                } else if (types.contains(Type.FLOAT) && types.contains(Type.DOUBLE)) {
                    return getTypeObject(Schema.create(Type.DOUBLE), object);

                } else {
                    final Row unionRow = (Row) object;
                    final List<Schema> unionTypes = schema.getTypes();
                    for (int index = 0; index < unionRow.size(); index++) {
                        final Object rowElement = unionRow.get(index);
                        if ((rowElement != null) && !rowElement.toString().equalsIgnoreCase("null")) {
                            return getTypeObject(unionTypes.get(index), rowElement);
                        }
                    }
                }
            }
        }
        return object;
    }
}
