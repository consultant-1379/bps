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

import static com.ericsson.component.aia.services.bps.core.common.Constants.URI;
import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.AttributeValueType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.InputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.OutputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.PathType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.StepType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.ToType;
import com.ericsson.component.aia.services.bps.core.common.Constants.ConfigType;
import com.ericsson.component.aia.services.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.services.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRule;
import com.ericsson.component.aia.services.bps.core.configuration.rule.BpsRuleValidationResult;
import com.ericsson.component.aia.services.bps.core.exception.BpsRuntimeException;
import com.ericsson.component.aia.services.bps.core.utils.BpsConfigurationRulesReader;

/**
 * This class will validate the flow xml before building bipe line
 */
public class BpsRuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsRuleEngine.class);

    private static final String BLOCK_NAME = "block.name";

    private BpsRuleEngine() {

    }

    /**
     * This method will validate the flow xml based on the rules configured in properties file
     *
     * @param flowDefinition
     *            representing flow xml
     */
    public static void validate(final FlowDefinition flowDefinition) {

        final List<String> failures = new ArrayList<>();

        checkArgument(flowDefinition != null, "Flow cannot be null.");

        LOGGER.trace("Validating flow definition");
        validateFlowDescription(flowDefinition, failures);

        LOGGER.trace("Validating inputs");
        validateInputType(flowDefinition.getInput(), ConfigType.INPUT, failures);

        LOGGER.trace("Validating outputs");
        validateOutputType(flowDefinition.getOutput(), ConfigType.OUTPUT, failures);

        LOGGER.trace("Validating steps");
        validateStepType(flowDefinition.getStep(), failures);

        LOGGER.trace("Validating path");
        if (failures.isEmpty()) {
            validatePaths(flowDefinition, failures);
        }

        if (!failures.isEmpty()) {
            logFailures(failures);
            throw new BpsRuntimeException("Flow xml validation failed. Please check log for more details");
        }
    }

    private static void validateFlowDescription(final FlowDefinition flowDefinition, final List<String> failures) {

        if (flowDefinition.getName() == null) {
            failures.add("FlowDefinition Name cannot be null.");
        }
        if (flowDefinition.getNs() == null) {
            failures.add("Namespace can not be null.");
        }
        if (flowDefinition.getVersion() == null) {
            failures.add("Version can not be null.");
        }
        if ((null == flowDefinition.getPath()) || flowDefinition.getPath().isEmpty()) {
            failures.add("Path's can not be null or empty.");
        }
    }

    private static void validateInputType(final List<InputType> inputs, final ConfigType configType, final List<String> failures) {
        if ((inputs == null) || inputs.isEmpty()) {
            failures.add(String.format("%ss can not be null or empty.", configType));
            return;
        }
        for (final InputType inputOutputType : inputs) {
            final String inputOutputName = inputOutputType.getName();
            if ((inputOutputName == null) || inputOutputName.isEmpty()) {
                failures.add(String.format("%s name can not be null or empty.", configType));
                continue;
            }
            final Properties properties = getInputTypeProperties(inputOutputType);
            validateProperties(properties, configType, failures);
        }
    }

    private static void validateOutputType(final List<OutputType> inputs, final ConfigType configType, final List<String> failures) {
        if ((inputs == null) || inputs.isEmpty()) {
            failures.add(String.format("%ss can not be null or empty.", configType));
            return;
        }
        for (final OutputType outputType : inputs) {
            final String inputOutputName = outputType.getName();
            if ((inputOutputName == null) || inputOutputName.isEmpty()) {
                failures.add(String.format("%s name can not be null or empty.", configType));
                continue;
            }
            final Properties properties = getOutputTypeProperties(outputType);
            validateProperties(properties, configType, failures);
        }
    }

    private static void validateStepType(final List<StepType> steps, final List<String> failures) {
        if ((steps == null) || steps.isEmpty()) {
            failures.add("steps can not be null or empty.");
            return;
        }
        for (final StepType step : steps) {
            final String stepName = step.getName();
            if ((stepName == null) || stepName.isEmpty()) {
                failures.add("Step name can not be null or empty.");
                continue;
            }
            final Properties properties = getStepTypeProperties(step);
            validateProperties(properties, ConfigType.STEP, failures);
        }
    }

    /**
     * validatePaths method check whether the provide xml has a valid Path definition or not.
     *
     * Following validations are there in place for Path tag: 1. From tag always refer input source 2. To tag maybe from input/output/steps 3. XML may
     * contain multiple From tags 4. Whatever declared in paths should be present in output & input adapters
     *
     * @param flowDefinition
     * @param failures
     */
    private static void validatePaths(final FlowDefinition flowDefinition, final List<String> failures) {

        if (flowDefinition.getPath() == null || flowDefinition.getPath().isEmpty()) {
            failures.add(String.format("Paths can not be null or empty."));
            return;
        }

        final Set<String> inputAdapterSet = new HashSet<>();

        for (final InputType inputType : flowDefinition.getInput()) {
            inputAdapterSet.add(inputType.getName());
        }

        final Set<String> stepOpTagsSet = getStepOpTags(flowDefinition);
        validateToAndFromsDefinedInPath(flowDefinition.getPath(), inputAdapterSet, stepOpTagsSet, failures);
    }

    private static void validateToAndFromsDefinedInPath(final List<PathType> paths, final Set<String> inputAdapterSet,
                                                        final Set<String> stepOpTagsSet, final List<String> failures) {
        for (final PathType path : paths) {

            if (null == path.getFrom() || null == path.getFrom().getUri() || path.getFrom().getUri().isEmpty()) {
                failures.add(String.format("Path's From tag can not be null or empty."));
                continue;
            }

            //From tag always refer input source
            if (!inputAdapterSet.contains(path.getFrom().getUri())) {
                failures.add(String.format("Path's From definition %s is not referring to input adapter, please check the Flow file.",
                        path.getFrom().getUri()));
            }

            for (final ToType toType : path.getTo()) {

                //This checks validates "Whatever declared in paths should be present in output, step & input adapters"
                if (!inputAdapterSet.contains(toType.getUri()) && !stepOpTagsSet.contains(toType.getUri())) {
                    failures.add(String.format(
                            "Could not able to locate paths To's tag - %s in input, output or steps definitions, please check the Flow file.",
                            toType.getUri()));
                }
            }
        }
    }

    private static Set<String> getStepOpTags(final FlowDefinition flowDefinition) {

        final Set<String> stepOpTagsSet = new HashSet<>();

        for (final StepType stepType : flowDefinition.getStep()) {
            stepOpTagsSet.add(stepType.getName());
        }

        for (final OutputType outputType : flowDefinition.getOutput()) {
            stepOpTagsSet.add(outputType.getName());
        }
        return stepOpTagsSet;
    }

    @SuppressWarnings("unchecked")
    private static void validateProperties(final Properties properties, final ConfigType configType, final List<String> failures) {
        final String uri = properties.getProperty(URI);
        if ((uri == null) || (uri.trim().length() <= 0)) {
            failures.add(String.format("Could not locate a valid uri for %s name %s, please check the Flow file.", configType,
                    properties.getProperty(BLOCK_NAME)));
            return;
        }
        if (!isSupportedUri(uri, configType)) {
            failures.add(String.format("Uri %s not supported for %s type with name, please check the Flow file.", uri, configType,
                    properties.getProperty(BLOCK_NAME)));
            return;
        }
        final String uriName = getUriName(configType, uri);
        final Properties mandatoryProperties = getMandatoryProperties(configType, uriName);
        final Set<String> mandatoryPropertiesSet = (Set<String>) (Set<?>) mandatoryProperties.keySet();
        final Set<String> inputOutputProperties = (Set<String>) (Set<?>) properties.keySet();
        final boolean isMandatoryPropertiesDefined = inputOutputProperties.containsAll(mandatoryPropertiesSet);
        if (!isMandatoryPropertiesDefined) {
            mandatoryPropertiesSet.removeAll(inputOutputProperties);
            failures.add(String.format("The properties defined for %s with name %s does not contain the following mandatory propeties %s", configType,
                    properties.getProperty(BLOCK_NAME), mandatoryPropertiesSet));
        } else {
            validateRulesForMandatoryProperties(mandatoryProperties, properties, failures);
        }
    }

    private static boolean isSupportedUri(final String uri, final ConfigType configType) {
        try {
            if ((ConfigType.INPUT == configType) || (ConfigType.OUTPUT == configType)) {
                IOURIS.findUri(uri);
            } else if (ConfigType.STEP == configType) {
                PROCESS_URIS.findUri(uri);
            }
            return true;
        } catch (final IllegalArgumentException argumentException) {
            return false;
        }
    }

    private static void validateRulesForMandatoryProperties(final Properties mandatoryProperties, final Properties properties,
                                                            final List<String> failures) {
        for (final Entry<Object, Object> entry : mandatoryProperties.entrySet()) {
            final String propertyName = (String) entry.getKey();
            final String ruleServiceContextName = (String) entry.getValue();
            final BpsRule bpsRule = (BpsRule) GenericServiceLoader.getService(BpsRule.class, ruleServiceContextName);
            final BpsRuleValidationResult result = bpsRule.validate(properties.getProperty(propertyName));
            if (!result.isSuccessful()) {
                LOGGER.info("Validation failed for parameter {} with value {} for rule {}", propertyName, properties.getProperty(propertyName),
                        ruleServiceContextName);
                failures.add(result.getFailureReason());
            }
        }

    }

    private static void logFailures(final List<String> failures) {
        for (final String failure : failures) {
            LOGGER.error(failure);
        }
    }

    private static Properties getMandatoryProperties(final ConfigType configType, final String uriName) {
        if (ConfigType.INPUT == configType) {
            return BpsConfigurationRulesReader.getIngressMandatoryProperties(uriName);
        } else if (ConfigType.OUTPUT == configType) {
            return BpsConfigurationRulesReader.getEgressMandatoryProperties(uriName);
        } else if (ConfigType.STEP == configType) {
            return BpsConfigurationRulesReader.getStepMandatoryProperties(uriName);
        }
        throw new BpsRuntimeException("Unsupported type only STEP, INPUT and OUTPUT supported");
    }

    private static String getUriName(final ConfigType configType, final String uri) {
        if ((ConfigType.INPUT == configType) || (ConfigType.OUTPUT == configType)) {
            return IOURIS.getUriName(uri);
        } else if (ConfigType.STEP == configType) {
            return PROCESS_URIS.getUriName(uri);
        }
        throw new BpsRuntimeException("Unsupported type only STEP, INPUT and OUTPUT supported");
    }

    private static Properties getOutputTypeProperties(final OutputType inputOutputType) {
        final Properties properties = new Properties();
        properties.put(BLOCK_NAME, inputOutputType.getName());
        final List<AttributeValueType> attributeValueTypes = inputOutputType.getAttribute();
        for (final AttributeValueType attributeValueType : attributeValueTypes) {
            properties.put(attributeValueType.getName(), attributeValueType.getValue());
        }
        return properties;
    }

    private static Properties getInputTypeProperties(final InputType inputOutputType) {
        final Properties properties = new Properties();
        properties.put(BLOCK_NAME, inputOutputType.getName());
        final List<AttributeValueType> attributeValueTypes = inputOutputType.getAttribute();
        for (final AttributeValueType attributeValueType : attributeValueTypes) {
            properties.put(attributeValueType.getName(), attributeValueType.getValue());
        }
        return properties;
    }

    private static Properties getStepTypeProperties(final StepType step) {
        final Properties properties = new Properties();
        properties.put(BLOCK_NAME, step.getName());
        final List<AttributeValueType> attributeValueTypes = step.getAttribute();
        for (final AttributeValueType attributeValueType : attributeValueTypes) {
            properties.put(attributeValueType.getName(), attributeValueType.getValue());
        }
        return properties;
    }
}