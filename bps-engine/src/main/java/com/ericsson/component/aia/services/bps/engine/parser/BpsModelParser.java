/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.services.bps.engine.parser;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition;
import com.ericsson.component.aia.itpf.common.modeling.schema.util.DtdModelHandlingUtil;
import com.ericsson.component.aia.itpf.common.modeling.schema.util.SchemaConstants;
import com.ericsson.component.aia.services.bps.core.pipe.BpsPipe;
import com.ericsson.component.aia.services.bps.core.pipe.BpsPipelineRunner;
import com.ericsson.component.aia.services.bps.engine.configuration.rule.engine.BpsRuleEngine;

/**
 * This class parses flow xml and builds pipe-line based on flow xml configuration.
 */
final public class BpsModelParser implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 580185855482292834L;

    /**
     * Instantiates a new bps model parser.
     */
    private BpsModelParser() {
    }

    /**
     * This methods accepts {@link InputStream} of flow xml and builds the pipe-line using flow xml configuration and returns the same for execution.
     *
     * @param reader
     *            the reader
     * @return configured pipe line
     */
    public static BpsPipe parseFlow(final Reader reader) {
        final FlowDefinition flowDefinition = getFlowDefinition(reader);
        BpsRuleEngine.validate(flowDefinition);
        final BpsPipe runner = BpsPipelineRunner.Builder.create().addFlow(flowDefinition).build();
        return runner;
    }

    /**
     * Populate flow definition based passed flow xml file.
     *
     * @param reader
     *            the reader
     * @return the flow definition
     */
    public static FlowDefinition getFlowDefinition(final Reader reader) {
        try {
            final Unmarshaller unmarshaller = DtdModelHandlingUtil.getUnmarshaller(SchemaConstants.AIA_FLOW);
            final Object root = unmarshaller.unmarshal(reader);
            final FlowDefinition flowDefinition = (FlowDefinition) root;
            return flowDefinition;
        } catch (final JAXBException jaxbexc) {
            throw new IllegalStateException("Invalid module - unable to parse it! Details: " + jaxbexc);
        }
    }
}