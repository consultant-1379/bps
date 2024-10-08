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
package com.ericsson.component.aia.services.bps.core.pipe;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.FlowDefinition;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.InputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.OutputType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.PathType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.StepType;
import com.ericsson.component.aia.itpf.common.modeling.flow.schema.gen.fbp_flow.ToType;
import com.ericsson.component.aia.services.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasink.BpsDataSinkConfigurationFactory;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.services.bps.core.service.configuration.datasource.BpsDataSourceConfigurationFactory;
import com.ericsson.component.aia.services.bps.core.step.BpsStep;
import com.ericsson.component.aia.services.bps.core.step.BpsStepFactory;

/**
 * PipelineRunner executes the operations present in the pipeline through Steps.
 */
public class BpsPipelineRunner implements BpsPipe {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsPipelineRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1666289193387607910L;

    /** The service. */
    private static ServiceLoader<BpsJobRunner> service;

    /** The ip adapters. */
    private BpsDataSourceAdapters ipAdapters;

    /** The op adapters. */
    private BpsDataSinkAdapters opAdapters;

    /** The pipe. */
    private List<PathType> pipe;

    /** The task seq. */
    private final List<String> taskSeq = new ArrayList<>();

    /** The name. */
    private String name;

    /** The version. */
    private String version;

    /** The namespace. */
    private String namespace;

    /** The path. */
    private final List<PathType> path;

    /** The steps. */
    private final List<BpsStep> steps;

    /**
     * Instantiates a new pipeline runner.
     *
     * @param builder
     *            the builder
     */
    private BpsPipelineRunner(final Builder builder) {
        name = builder.name;
        version = builder.version;
        namespace = builder.namespace;
        path = builder.path;
        steps = builder.steps;
    }

    /**
     * The Class Builder.
     */
    public static class Builder {

        /** The name. */
        private String name;

        /** The version. */
        private String version;

        /** The namespace. */
        private String namespace;

        /** The path. */
        private List<PathType> path;

        /** The ip adapters. */
        private BpsDataSourceAdapters ipAdapters;

        /** The op adapters. */
        private BpsDataSinkAdapters opAdapters;

        /** The steps. */
        private List<BpsStep> steps;

        /**
         * Creates the.
         *
         * @return the builder
         */
        public static Builder create() {
            return new Builder();
        }

        /**
         * Adds the flow and creates a builder object.
         *
         * @param flowDefinition
         *            the flow definition
         * @return the builder
         */
        public Builder addFlow(final FlowDefinition flowDefinition) {
            LOG.trace("Entering the addFlow method");
            return addName(flowDefinition.getName()).addNamespace(flowDefinition.getNs()).addVersion(flowDefinition.getVersion())
                    .addPathType(flowDefinition.getPath()).addInputs(flowDefinition.getInput()).addOutputs(flowDefinition.getOutput())
                    .addSteps(flowDefinition.getStep());
        }

        /**
         * Adds the name.
         *
         * @param name
         *            the name
         * @return the builder
         */
        private Builder addName(final String name) {
            this.name = name;
            LOG.trace("Exiting the addName method");
            return this;
        }

        /**
         * Adds the namespace.
         *
         * @param namespace
         *            the namespace
         * @return the builder
         */
        private Builder addNamespace(final String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Adds the version.
         *
         * @param version
         *            the version
         * @return the builder
         */
        private Builder addVersion(final String version) {
            this.version = version;
            return this;
        }

        /**
         * Adds the path type.
         *
         * @param path
         *            the path
         * @return the builder
         */
        private Builder addPathType(final List<PathType> path) {
            this.path = path;
            return this;
        }

        /**
         * Adds the inputs.
         *
         * @param inputList
         *            the input list
         * @return the builder
         */
        private Builder addInputs(final List<InputType> inputList) {
            LOG.trace("Entering the addInputs method");
            ipAdapters = new BpsDataSourceAdapters();
            for (final InputType inputType : inputList) {
                ipAdapters.addBpsDataSourceConfiguration(BpsDataSourceConfigurationFactory.create(inputType));
            }
            LOG.trace("Exiting the addInputs method");
            return this;
        }

        /**
         * Adds the outputs.
         *
         * @param outputList
         *            the output list
         * @return the builder
         */
        private Builder addOutputs(final List<OutputType> outputList) {
            LOG.trace("Entering the addOutputs method");
            opAdapters = new BpsDataSinkAdapters();
            for (final OutputType outputType : outputList) {
                opAdapters.addBpsDataSinkConfiguration(BpsDataSinkConfigurationFactory.create(outputType));
            }
            LOG.trace("Exiting the addOutputs method");
            return this;
        }

        /**
         * Adds the steps.
         *
         * @param steps
         *            the steps
         * @return the builder
         */
        private Builder addSteps(final List<StepType> steps) {
            LOG.trace("Entering the addSteps method");
            if (steps.size() > 1) {
                throw new IllegalArgumentException("Currently only supporting single processing step");
            }
            this.steps = new LinkedList<>();
            for (final StepType stepType : steps) {
                final BpsStep step = BpsStepFactory.create(stepType);
                step.getStepHandler().initialize(ipAdapters, opAdapters, step.getconfiguration());
                this.steps.add(step);
            }
            LOG.trace("Exiting the addSteps method");
            return this;
        }

        /**
         * Builds the pipe.
         *
         * @return the pipe
         */
        public BpsPipe build() {
            LOG.trace("Entering the build method");
            LOG.trace("Exiting the build method");
            return new BpsPipelineRunner(this);
        }
    }

    /**
     * This method sets path(flow.xml Paths).
     */
    @Override
    public void setFlow(final List<PathType> path) {
        LOG.trace("Entering the setFlow method");
        pipe = path;
        taskSeq.add(path.get(0).getFrom().getUri());
        final List<ToType> tos = path.get(0).getTo();
        for (final ToType toType : tos) {
            taskSeq.add(toType.getUri());
        }
        LOG.trace("Exiting the setFlow method");
    }

    /**
     * Execute method runs all Step of a pipeline job.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        for (final BpsStep step : steps) {
            step.execute();
        }
        LOG.trace("Exiting the execute method");
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the pipe.
     *
     * @return the pipe
     */
    public List<PathType> getPipe() {
        return pipe;
    }

    /**
     * Gets the task seq.
     *
     * @return the taskSeq
     */
    public List<String> getTaskSeq() {
        return taskSeq;
    }

    /**
     * Gets the version.
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the service.
     *
     * @return the service
     */
    public static ServiceLoader<BpsJobRunner> getService() {
        return service;
    }

    /**
     * Sets the service.
     *
     * @param service
     *            the service to set
     */
    public static void setService(final ServiceLoader<BpsJobRunner> service) {
        BpsPipelineRunner.service = service;
    }

    /**
     * Gets the ip adapters.
     *
     * @return the ip adapters
     */
    public BpsDataSourceAdapters getIpAdapters() {
        return ipAdapters;
    }

    /**
     * Gets the op adapters.
     *
     * @return the op adapters
     */
    public BpsDataSinkAdapters getOpAdapters() {
        return opAdapters;
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets the path.
     *
     * @return the path
     */
    public List<PathType> getPath() {
        return path;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public void setNAMESPACE(final String nameSpace) {
        namespace = nameSpace;

    }

    @Override
    public void setVERSION(final String version) {
        this.version = version;
    }

    /**
     * This operation will do the clean up for Job's Step handlers.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        // clean steps
        for (final BpsStep step : steps) {
            step.cleanUp();
        }
        LOG.trace("Exiting the cleanUp method");
    }

    @Override
    public String toString() {
        return "PipelineRunner [ipAdapters=" + ipAdapters + ", opAdapters=" + opAdapters + ", pipe=" + pipe + ", taskSeq=" + taskSeq + ", name="
                + name + ", version=" + version + ", namespace=" + namespace + ", path=" + path + ", steps=" + steps + "]";
    }

}
