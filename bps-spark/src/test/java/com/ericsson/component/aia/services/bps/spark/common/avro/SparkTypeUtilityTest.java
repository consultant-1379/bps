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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.junit.Test;

import scala.collection.JavaConversions;

public class SparkTypeUtilityTest {
    public static final String USER_SCHEMA = "{ \"type\":\"record\"," + "\"name\":\"myrecord\"," + "\"fields\":["
            + "  { \"name\":\"int1\", \"type\": {\"type\": \"array\", \"items\": \"long\"} }]}";

    @Test
    public void shouldConvertStringToBooelan() {

        final Object trueValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), "true");
        assertEquals(Boolean.class, trueValue.getClass());
        assertEquals(true, trueValue);

        final Object falseValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), "false");
        assertEquals(Boolean.class, falseValue.getClass());
        assertEquals(false, falseValue);

        final Object falseValueForNumbericTrue = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), "1");
        assertEquals(Boolean.class, falseValueForNumbericTrue.getClass());
        assertEquals(true, falseValueForNumbericTrue);

        final Object falseValueForNumbericFalse = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), "0");
        assertEquals(Boolean.class, falseValueForNumbericFalse.getClass());
        assertEquals(false, falseValueForNumbericFalse);

        final Object falseValueForAlphaNumberic = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), "1x");
        assertEquals(Boolean.class, falseValueForAlphaNumberic.getClass());
        assertEquals(false, falseValueForAlphaNumberic);

    }

    @Test
    public void shouldConvertStringToByte() {
        final String test = "test";
        final Object trueValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BYTES), test);
        assertEquals(ByteBuffer.wrap(String.valueOf(test).getBytes()), trueValue);

    }

    @Test
    public void shouldConvertStringToNumber() {

        final Object intValue = SparkTypeUtility.getTypeObject(Schema.create(Type.INT), "1");
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object expectedNullforInt = SparkTypeUtility.getTypeObject(Schema.create(Type.INT), "1x");
        assertEquals(null, expectedNullforInt);

        final Object longValue = SparkTypeUtility.getTypeObject(Schema.create(Type.LONG), "1");
        assertEquals(Long.class, longValue.getClass());
        assertEquals(1l, longValue);

        final Object expectedNullforLong = SparkTypeUtility.getTypeObject(Schema.create(Type.LONG), "1x");
        assertEquals(null, expectedNullforLong);

        final Object doubleValue = SparkTypeUtility.getTypeObject(Schema.create(Type.DOUBLE), "1");
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1D, doubleValue);

        final Object expectedNullforDouble = SparkTypeUtility.getTypeObject(Schema.create(Type.DOUBLE), "1x");
        assertEquals(null, expectedNullforDouble);

        final Object floatValue = SparkTypeUtility.getTypeObject(Schema.create(Type.FLOAT), "1");
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1F, floatValue);

        final Object expectedNullforFloat = SparkTypeUtility.getTypeObject(Schema.create(Type.FLOAT), "1x");
        assertEquals(null, expectedNullforFloat);

        final List<?> listArrayValue = (List<?>) SparkTypeUtility.getTypeObject(Schema.createArray(Schema.create(Type.FLOAT)),
                JavaConversions.collectionAsScalaIterable(Arrays.asList("1")));
        assertEquals(1F, listArrayValue.get(0));

        final Object expectedString = SparkTypeUtility.getTypeObject(Schema.create(Type.STRING), '1');
        assertEquals("1", expectedString);

    }

    @Test
    public void shouldConvertIntegerToNumber() {

        final Object intValue = SparkTypeUtility.getTypeObject(Schema.create(Type.INT), 1);
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object expectIntValue = SparkTypeUtility.getTypeObject(Schema.create(Type.INT), new Byte("1"));
        assertEquals(Integer.class, expectIntValue.getClass());
        assertEquals(1, expectIntValue);

        final Object longVlaue = SparkTypeUtility.getTypeObject(Schema.create(Type.LONG), 1);
        assertEquals(Long.class, longVlaue.getClass());
        assertEquals(1L, longVlaue);

        final Object expectedLongVlaue = SparkTypeUtility.getTypeObject(Schema.create(Type.LONG), 1L);
        assertEquals(Long.class, expectedLongVlaue.getClass());
        assertEquals(1L, expectedLongVlaue);

        final Object floatValue = SparkTypeUtility.getTypeObject(Schema.create(Type.FLOAT), 1);
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1.0F, floatValue);

        final Object doubleValue = SparkTypeUtility.getTypeObject(Schema.create(Type.DOUBLE), 1);
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1.0D, doubleValue);

        final Object stringValue = SparkTypeUtility.getTypeObject(Schema.create(Type.STRING), 1);
        assertEquals(String.class, stringValue.getClass());
        assertEquals("1", stringValue);

        final Object booleanValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), 1);
        assertEquals(Boolean.class, booleanValue.getClass());
        assertEquals(true, booleanValue);

        final Object byteValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BYTES), 1);
        assertEquals(ByteBuffer.wrap(String.valueOf("1").getBytes()), byteValue);

        final List<?> arrayValue = (List<?>) SparkTypeUtility.getTypeObject(Schema.createArray(Schema.create(Type.INT)),
                JavaConversions.collectionAsScalaIterable(Arrays.asList("10000")));
        assertEquals(10000, arrayValue.get(0));
    }

    @Test
    public void shouldConvertFloatToNumber() {

        final Object intValue = SparkTypeUtility.getTypeObject(Schema.create(Type.INT), 1f);
        assertEquals(Integer.class, intValue.getClass());
        assertEquals(1, intValue);

        final Object longVlaue = SparkTypeUtility.getTypeObject(Schema.create(Type.LONG), 1f);
        assertEquals(Long.class, longVlaue.getClass());
        assertEquals(1L, longVlaue);

        final Object floatValue = SparkTypeUtility.getTypeObject(Schema.create(Type.FLOAT), 1f);
        assertEquals(Float.class, floatValue.getClass());
        assertEquals(1.0F, floatValue);

        final Object expectedFloatValue = SparkTypeUtility.getTypeObject(Schema.create(Type.FLOAT), 1D);
        assertEquals(Float.class, expectedFloatValue.getClass());
        assertEquals(1.0F, expectedFloatValue);

        final Object doubleValue = SparkTypeUtility.getTypeObject(Schema.create(Type.DOUBLE), 1f);
        assertEquals(Double.class, doubleValue.getClass());
        assertEquals(1.0D, doubleValue);

        final Object expectDoubleValue = SparkTypeUtility.getTypeObject(Schema.create(Type.DOUBLE), 1d);
        assertEquals(Double.class, expectDoubleValue.getClass());
        assertEquals(1.0D, expectDoubleValue);

        final Object stringValue = SparkTypeUtility.getTypeObject(Schema.create(Type.STRING), 1f);
        assertEquals(String.class, stringValue.getClass());
        assertEquals("1.0", stringValue);

        final Object booleanValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BOOLEAN), 1f);
        assertEquals(Boolean.class, booleanValue.getClass());
        assertEquals(true, booleanValue);

        final Object byteValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BYTES), 1f);
        assertEquals(ByteBuffer.wrap(String.valueOf(Float.valueOf(1f)).getBytes()), byteValue);

        final List<?> arrayValue = (List<?>) SparkTypeUtility.getTypeObject(Schema.createArray(Schema.create(Type.DOUBLE)),
                JavaConversions.collectionAsScalaIterable(Arrays.asList(1d)));
        assertEquals(1.0D, arrayValue.get(0));
    }

    @Test
    public void shouldConvertByteArrayToByteBuffer() {
        final byte[] bytes = { 0x01, 0x02 };
        final Object trueValue = SparkTypeUtility.getTypeObject(Schema.create(Type.BYTES), bytes);
        assertEquals(ByteBuffer.wrap(bytes), trueValue);
    }

    @Test
    public void shouldConvertStringToString() {
        final String test = "test";
        final Object trueValue = SparkTypeUtility.getTypeObject(Schema.create(Type.STRING), test);
        assertEquals(String.class, trueValue.getClass());
        assertEquals(test, trueValue);
    }

    @Test
    public void shouldConvertGenericRow() {
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(USER_SCHEMA);
        final Row row = new GenericRow(new Object[] { JavaConversions.collectionAsScalaIterable(Arrays.asList(1l)) });

        final Record value = (Record) SparkTypeUtility.getTypeObject(schema, row);
        assertEquals(1l, ((List) value.get("int1")).get(0));
    }

}
