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

/**
 * JUNIT class to test BpsDataFormatRule
 */
public class BpsAttributeRuleTest {

    private BpsAttributeRule bpsDataFormatRule = new BpsAttributeRule();

    @Test
    public void shouldReturnTrueWhenValidWordIsProvided() {
        assertTrue(bpsDataFormatRule.validate("xyzS2_").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("org.apache.kafka.clients.consumer.RangeAssignor").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("localhost:56644").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("rc/test/resources/avro/").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("rc\\test\\resources\\avro\\").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("org.apache.kafka.common.serialization.Serdes$StringSerde").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("asr-1:8080").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("kafka-stream-topic85.1256").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("-stream-topic85.1256").isSuccessful());
        assertTrue(bpsDataFormatRule.validate("asr-1:8080,asr-2:9090").isSuccessful());
		assertTrue(bpsDataFormatRule.validate("192.168.10.2:8080,asr-2:9090").isSuccessful());
    }

    @Test
    public void shouldReturnFalseWhenEmptyStringIsProvided() {
        assertFalse(bpsDataFormatRule.validate(" ").isSuccessful());
    }

    @Test
    public void shouldReturnFalseWhenWordHasEmptyString() {
        assertFalse(bpsDataFormatRule.validate("xyz S2_").isSuccessful());
    }

}
