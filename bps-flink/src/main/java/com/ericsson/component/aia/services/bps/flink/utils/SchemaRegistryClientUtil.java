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
package com.ericsson.component.aia.services.bps.flink.utils;

import static com.ericsson.component.aia.services.bps.core.common.Constants.SCHEMA_REGISTRY_ADDRESS;
import static com.ericsson.component.aia.services.bps.core.common.Constants.SCHEMA_REGISTRY_CACHE_MAXIMUM_SIZE;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.impl.SchemaRegistryClientFactory;

/**
 * Utility class providing apis for finding and returning SchemaRegistryClient properties.
 */
public class SchemaRegistryClientUtil {

    private SchemaRegistryClientUtil() {

    }

    /**
     * This method will return SchemaRegistryClient properties if all the required properties are present in the passed parameter
     *
     * @param properties
     *            of configuration provided
     * @return SchemaRegistryClient properties
     */
    public static Map<String, String> getSchemaRegistryClientProperties(final Properties properties) {
        final Map<String, String> schemaRegistryClientProperties = new HashMap<String, String>();
        final String schemaRegistryAddress = properties.getProperty(SCHEMA_REGISTRY_ADDRESS);
        if (StringUtils.isNotBlank(schemaRegistryAddress)) {
            schemaRegistryClientProperties.put(SCHEMA_REGISTRY_ADDRESS, schemaRegistryAddress);
            final String schemaRegistryCacheSize = properties.getProperty(SCHEMA_REGISTRY_CACHE_MAXIMUM_SIZE);
            if (StringUtils.isNotBlank(schemaRegistryCacheSize)) {
                schemaRegistryClientProperties.put(SCHEMA_REGISTRY_CACHE_MAXIMUM_SIZE, schemaRegistryCacheSize);
            }
        }
        return schemaRegistryClientProperties;
    }

    /**
     * This method will return SchemaRegistryClient to use
     *
     * @param properties
     *            of configuration provided
     * @return SchemaRegistryClient to use
     */
    public static SchemaRegistryClient getSchemaRegistryClient(final Properties properties) {
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClient.INSTANCE;
        final Map<String, String> schemaRegistryClientPropertiesMap = getSchemaRegistryClientProperties(properties);
        if (!schemaRegistryClientPropertiesMap.isEmpty()) {
            final Properties schemaRegistryClientProperties = new Properties();
            schemaRegistryClientProperties.putAll(schemaRegistryClientPropertiesMap);
            schemaRegistryClient = SchemaRegistryClientFactory.newSchemaRegistryClientInstance(schemaRegistryClientProperties);
        }
        return schemaRegistryClient;
    }
}
