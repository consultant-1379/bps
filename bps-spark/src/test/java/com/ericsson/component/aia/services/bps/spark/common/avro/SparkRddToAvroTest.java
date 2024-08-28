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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.exception.SchemaRetrievalException;
import com.ericsson.component.aia.model.registry.impl.RegisteredSchema;
import com.ericsson.component.aia.services.bps.core.exception.GenericRecordConversionException;

import scala.collection.JavaConversions;

public class SparkRddToAvroTest {

    private final static String EVENT_TYPE = "celltrace.s.ab11.INTERNAL_PROC_UE_CTXT_RELEASE";
    private final static String COMPLEX_DATA_TYPE = "celltrace.s.ab11.COMPLEX_DATA_SCHEMA";
    private final static String COMPLEX_DATA_TYPE_2 = "celltrace.s.ab11.COMPLEX_DATA_SCHEMA2";

    private final static String SCHEMA_REGISTRY_ADDRESS = "src/test/resources/avro/";
    private static Row eventOneRowWithSchema, eventTwoRowWithSchema, eventThreeRowWithSchema;

    private static RegisteredSchema eventOneSchema, eventTwoSchema, eventThreeSchema;
    private static SchemaRegistryClient schemaRegistryClient;

    private static String[] eventOneFieldName, eventTwoFieldName, eventThreeFieldName;

    @BeforeClass
    public static void init() throws SchemaRetrievalException {
        System.setProperty("schemaRegistry.address", SCHEMA_REGISTRY_ADDRESS);

        initEventOneSqlRow();
        initEventTwoSqlRow();
        initEventThreeSqlRow();

        schemaRegistryClient = SchemaRegistryClient.INSTANCE;

        eventOneSchema = schemaRegistryClient.lookup(EVENT_TYPE);
        eventTwoSchema = schemaRegistryClient.lookup(COMPLEX_DATA_TYPE);
        eventThreeSchema = schemaRegistryClient.lookup(COMPLEX_DATA_TYPE_2);

        eventOneFieldName = eventOneRowWithSchema.schema().fieldNames();
        eventTwoFieldName = eventTwoRowWithSchema.schema().fieldNames();
        eventThreeFieldName = eventThreeRowWithSchema.schema().fieldNames();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldVerifyTransformRddToAvroRddMapCall() throws GenericRecordConversionException, SchemaRetrievalException {
        final JavaRDD<Row> rdd = Mockito.mock(JavaRDD.class);
        Mockito.when(rdd.first()).thenReturn(eventOneRowWithSchema);
        Mockito.when(rdd.map(Mockito.any(Function.class))).thenReturn(rdd);

        SparkRddToAvro.transformRddToAvroRdd(rdd, SCHEMA_REGISTRY_ADDRESS, 5000, eventOneFieldName, eventOneSchema);

        Mockito.verify(rdd, Mockito.times(1)).map(Mockito.any(Function.class));
    }

    @Test
    public void shouldVerifyTransformRowToAvroRecordWithRequiredDataType() throws GenericRecordConversionException, SchemaRetrievalException {
        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        final List<Field> fields = eventOneSchema.getSchema().getFields();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema());
        }

        final GenericRecord record = SparkRddToAvro.transformRowToAvroRecord(eventOneRowWithSchema, eventOneFieldName,
                new AvroConversionData(eventOneSchema.getSchemaId(), eventOneSchema.getSchema()));

        assertEquals("_NE", record.get("_NE"));
        assertEquals(1L, record.get("SCANNER_ID"));
    }

    @Test(expected = GenericRecordConversionException.class)
    public void shouldTransformRowToAvroRecordRaiseGenericRecordException() throws GenericRecordConversionException, SchemaRetrievalException {
        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        final List<Field> fields = eventOneSchema.getSchema().getFields();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema());
        }

        final StructField[] structFields = { new StructField("eventName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventType", DataTypes.StringType, true, Metadata.empty()) };
        final GenericRowWithSchema row = new GenericRowWithSchema(new Object[] { "_NE", null }, new StructType(structFields));
        final String[] fieldNames = row.schema().fieldNames();

        SparkRddToAvro.transformRowToAvroRecord(row, fieldNames, new AvroConversionData(eventOneSchema.getSchemaId(), eventOneSchema.getSchema()));
    }

    @Test
    public void shouldVerifyTransformRowToAvroRecordWithRequiredDataType2() throws GenericRecordConversionException, SchemaRetrievalException {

        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        final List<Field> fields = eventOneSchema.getSchema().getFields();
        final Long id = eventOneSchema.getSchemaId();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema());
        }

        final String[] fieldNames = eventOneRowWithSchema.schema().fieldNames();
        final GenericRecord record = SparkRddToAvro.transformRowToAvroRecord(eventOneRowWithSchema, fieldNames,
                new AvroConversionData(eventOneSchema.getSchemaId(), eventOneSchema.getSchema()));
        assertEquals("_NE", record.get("_NE"));
        assertEquals(1L, record.get("SCANNER_ID"));
    }

    @Test
    public void shouldVerifyTransformRowToAvroRecordWithComplexDataType() throws GenericRecordConversionException, SchemaRetrievalException {
        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        final List<Field> fields = eventTwoSchema.getSchema().getFields();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema());
        }

        final GenericRecord record = SparkRddToAvro.transformRowToAvroRecord(eventTwoRowWithSchema, eventTwoFieldName,
                new AvroConversionData(eventTwoSchema.getSchemaId(), eventTwoSchema.getSchema()));

        assertEquals("_NE", record.get("_NE"));
        assertEquals(1L, ((GenericRecord) ((List) record.get("STRUCT_ARRAY_DATA")).get(0)).get("int1"));
        assertEquals("aaa", ((List) record.get("PRIMITIVE_ARRAY_DATA")).get(0));
    }

    @Test
    public void shouldVerifyTransformRowToAvroRecordWithComplexDataType2() throws GenericRecordConversionException, SchemaRetrievalException {
        final Map<String, Schema> avroSchemaFields = new HashMap<>();
        final List<Field> fields = eventThreeSchema.getSchema().getFields();
        for (final Field field : fields) {
            avroSchemaFields.put(field.name(), field.schema());
        }

        final GenericRecord record = SparkRddToAvro.transformRowToAvroRecord(eventThreeRowWithSchema, eventThreeFieldName,
                new AvroConversionData(eventThreeSchema.getSchemaId(), eventThreeSchema.getSchema()));

        assertEquals("_NE", record.get("_NE"));
        assertEquals(11L, ((Map) ((List) record.get("MAP_ARRAY_DATA")).get(0)).get("time"));
        assertEquals(333L, ((List) record.get("PRIMITIVE_ARRAY_DATA")).get(0));
    }

    private static void initEventOneSqlRow() {
        final StructField[] structFields = { new StructField("_NE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SCANNER_ID", DataTypes.StringType, true, Metadata.empty()) };
        eventOneRowWithSchema = new GenericRowWithSchema(new Object[] { "_NE", "1" }, new StructType(structFields));
    }

    private static void initEventTwoSqlRow() {
        final DataType timeStruct = DataTypes
                .createStructType(new StructField[] { new StructField("int1", DataTypes.StringType, true, Metadata.empty()) });

        final StructField[] structFields = { new StructField("_NE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("STRUCT_ARRAY_DATA", DataTypes.createArrayType(timeStruct), true, Metadata.empty()),
                new StructField("PRIMITIVE_ARRAY_DATA", DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty()) };

        final Row complexDataType = new GenericRow(new Object[] { 1l });
        eventTwoRowWithSchema = new GenericRowWithSchema(new Object[] { "_NE", JavaConversions.collectionAsScalaIterable(singleton(complexDataType)),
                JavaConversions.collectionAsScalaIterable(singleton("aaa")) }, new StructType(structFields));

    }

    private static void initEventThreeSqlRow() {
        final DataType timeStruct = DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType);
        final StructField[] structFields = { new StructField("_NE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("MAP_ARRAY_DATA", DataTypes.createArrayType(timeStruct), true, Metadata.empty()),
                new StructField("PRIMITIVE_ARRAY_DATA", DataTypes.createArrayType(DataTypes.LongType), true, Metadata.empty()) };

        eventThreeRowWithSchema = new GenericRowWithSchema(new Object[] { "_NE",
                JavaConversions.collectionAsScalaIterable(singletonList(JavaConversions.mapAsScalaMap(singletonMap("time", 11l)))),
                JavaConversions.collectionAsScalaIterable(singleton(333l)) }, new StructType(structFields));
    }

}
