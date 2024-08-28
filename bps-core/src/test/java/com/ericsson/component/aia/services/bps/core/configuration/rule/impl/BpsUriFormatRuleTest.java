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
package com.ericsson.component.aia.services.bps.core.configuration.rule.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BpsUriFormatRuleTest {

    @Test
    public void testValidURIFormat() {
        final BpsUriFormatRule bpsUriFormatRule = new BpsUriFormatRule();
        assertTrue(bpsUriFormatRule.validate("spark-sql://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("spark-batch://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("spark-streaming://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("flink-streaming://CALLDROP").isSuccessful());

        assertTrue(bpsUriFormatRule.validate("amq://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("drill://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("file://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("hive://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("kafka://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("alluxio://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("zmq://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("hdfs://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("jdbc://CALLDROP").isSuccessful());
        assertTrue(bpsUriFormatRule.validate("web-socket://CALLDROP").isSuccessful());
    }

    @Test
    public void testInValidURIFormat() {
        final BpsUriFormatRule bpsUriFormatRule = new BpsUriFormatRule();
        assertFalse(bpsUriFormatRule.validate("spark-sql:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("spark-batch:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("spark-streaming:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("flink-streaming:://CALLDROP").isSuccessful());

        assertFalse(bpsUriFormatRule.validate("amq:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("drill:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("file:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("hive:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("kafka:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("alluxio:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("zmq:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("hdfs:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("jdbc:://CALLDROP").isSuccessful());
        assertFalse(bpsUriFormatRule.validate("web-socket:://CALLDROP").isSuccessful());
    }

    @Test
    public void testSpecialCases() {
        final String jdbc = "JDBC://jdbc:postgresql://127.0.0.1:5432/saj";
        final String kafka = "kafka://ashish?type=avro&filter=*RRC*";
        final BpsUriFormatRule bpsUriFormatRule = new BpsUriFormatRule();
        assertTrue(bpsUriFormatRule.validate(jdbc).isSuccessful());
        assertTrue(bpsUriFormatRule.validate(kafka).isSuccessful());
    }
}
