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
package com.ericsson.component.aia.services.bps.core.step;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.StepType;
import com.ericsson.component.aia.services.bps.core.service.MockSparkJobRunner;

public class BpsStepFactoryTest {

    final StepType stepType = Mockito.mock(StepType.class);

    @Before
    public void setup() {
        final List<AttributeValueType> lstAttTypes = new ArrayList<>();
        final AttributeValueType mode = new AttributeValueType();
        mode.setName("master.url");
        mode.setValue("local[*]");
        lstAttTypes.add(mode);
        final AttributeValueType uri = new AttributeValueType();
        uri.setName("uri");
        uri.setValue("spark-batch://sales-analysis");
        lstAttTypes.add(uri);
        Mockito.when(stepType.getName()).thenReturn("spark-step");
        Mockito.when(stepType.getAttribute()).thenReturn(lstAttTypes);
    }

    @Test
    public void testGetStepHandler() {
        final BpsStep create = BpsStepFactory.create(stepType);
        Assert.assertEquals(MockSparkJobRunner.class, create.getStepHandler().getClass());
    }
}
