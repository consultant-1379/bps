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
package com.ericsson.component.aia.services.bps.engine.configuration.rule.engine;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.engine.parser.BpsModelParser;

@RunWith(Parameterized.class)
public class BpsRuleEngineTest {

    @Parameter(0)
    public String xmlPath;
    @Parameter(1)
    public String scenario;
    @Parameter(2)
    public Class<? extends Exception> expectedException;
    @Parameter(3)
    public String expectedExceptionMsg;

    private FlowDefinition flowDefinition;

    private static final String EXCEPTION_MESSAGE = "Flow xml validation failed. Please check log for more details";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Parameters(name = "{index}: {1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Different xml validation scenarios:
                { "valid_flow.xml", "Valid Xml", null, null }, // Valid scenario
                { "ip_adapter_not_present_in_path.xml", "IO adapter exists but not present in path", null, null }, // Valid scenario
                { "ignore_duplicate_io_adapter.xml", "Ignore duplicate io adapters in path", null, null }, // Valid scenario
                { "blank_spaces_io_names.xml", "Blank spaces in all tags of the xml", BpsRuntimeException.class, EXCEPTION_MESSAGE }, // Invalid scenario
                { "no_valid_input_adapter.xml", "Input adapter is not configured", BpsRuntimeException.class, EXCEPTION_MESSAGE }, // Invalid scenario
                { "missing_io_adapter_in_path.xml", "Path contains input adapter that does exists", BpsRuntimeException.class, EXCEPTION_MESSAGE }, // Invalid scenario
                { "invalid_fromTag_in_path.xml", "Path's From tag starts with something other than input adapter", BpsRuntimeException.class,
                        EXCEPTION_MESSAGE }, // Invalid scenario
                { "no_fromTag.xml", "Path's does not contain From tag", BpsRuntimeException.class, EXCEPTION_MESSAGE },// Invalid scenario
        });
    }

    @Before
    public void setUp() throws Exception {
        final Reader reader = new FileReader(getFile("xmls/" + xmlPath));
        flowDefinition = BpsModelParser.getFlowDefinition(reader);
    }

    private File getFile(final String fileName) {
        final ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(fileName).getFile());
    }

    /**
     * testBpsRuleEngineScenarios does not have assert clause. But it expects BpsRuleEngine.validate to pass for a valid scenario and for a invalid
     * xml BpsRuleEngine.validate throws an runtime exception
     *
     * Test method for
     * {@link com.ericsson.component.aia.services.bps.engine.configuration.rule.engine.BpsRuleEngine#validate(com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition)}
     */
    @Test
    public void testBpsRuleEngineScenarios() {
        //setup expected exception
        if (expectedException != null) {
            thrown.expect(expectedException);
            thrown.expectMessage(expectedExceptionMsg);
        }
        BpsRuleEngine.validate(flowDefinition);
    }
}
