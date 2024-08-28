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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.SinkType;

/**
 * Created on 10/26/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBpsDataSinkConfigurationTest {

    private final DefaultBpsDataSinkConfiguration defaultBpsDataSinkConfiguration = new DefaultBpsDataSinkConfiguration();

    private final AttributeValueType attributeValueType = new AttributeValueType();

    private List<AttributeValueType> attribute;

    final Properties props = new Properties();

    @Before
    public void setUp() {

        attributeValueType.setName("uri");
        attributeValueType.setValue("hive://hive_ctr_events");
        attribute = new ArrayList<>();
        attribute.add(attributeValueType);
        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }
    }

    @Test
    public void testConfigure() {
        defaultBpsDataSinkConfiguration.configure("hive-input", props, Collections.<SinkType> emptyList());
        assertEquals(defaultBpsDataSinkConfiguration.getDataSinkContextName(), "hive-input");
        assertTrue(defaultBpsDataSinkConfiguration.getDataSinkConfiguration().size() == 1);
        assertEquals(defaultBpsDataSinkConfiguration.toString(), "[OutputName=HIVE contextName=hive-input]");

    }
}
