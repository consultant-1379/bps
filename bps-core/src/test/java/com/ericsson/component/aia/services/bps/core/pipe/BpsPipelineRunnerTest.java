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
package com.ericsson.component.aia.services.bps.core.pipe;

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition;
import com.ericsson.component.aia.itpf.common.modeling.schema.util.DtdModelHandlingUtil;
import com.ericsson.component.aia.itpf.common.modeling.schema.util.SchemaConstants;

/**
 * Validate PipelineRunner.
 */
public class BpsPipelineRunnerTest {

    private FlowDefinition flowDefinition;

    @Before
    public void setup() throws JAXBException {
        final Unmarshaller unmarshaller = DtdModelHandlingUtil.getUnmarshaller(SchemaConstants.AIA_FLOW);
        final Object root = unmarshaller.unmarshal(new File("src/test/resources/bps_flow_example.xml"));
        flowDefinition = (FlowDefinition) root;
    }

    @Test
    public void testBpsPipelineRunner() {
        final BpsPipelineRunner runner = (BpsPipelineRunner) BpsPipelineRunner.Builder.create().addFlow(flowDefinition).build();
        runner.execute();
        runner.cleanUp();
        assertEquals("CSLSolutionSet", runner.getName());
        assertEquals("com.ericsson.oss.services", runner.getNamespace());
        assertEquals("1.0.0", runner.getVersion());
    }
}