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
package com.ericsson.component.aia.services.bps.core.service.configuration.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.InputType;

/**
 * Created on 10/24/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class BpsDataSourceConfigurationTest {

    @Mock
    private InputType inputType;

    private final AttributeValueType attributeValueType = new AttributeValueType();

    private List<AttributeValueType> attribute;

    @Before
    public void setUp() {
        attributeValueType.setName("uri");
        attributeValueType.setValue("JDBC://jdbc:postgresql://localhost:5432/postgres");
        attribute = new ArrayList<>();
        attribute.add(attributeValueType);
    }

    @Test
    public void testCreate() {
        Mockito.when(inputType.getName()).thenReturn("jdbc-input");
        Mockito.when(inputType.getAttribute()).thenReturn(attribute);
        assertTrue(!BpsDataSourceConfigurationFactory.create(inputType).toString().isEmpty());
        assertEquals(BpsDataSourceConfigurationFactory.create(inputType).toString(), "[InputName=JDBC contextName=jdbc-input]");

    }
}
