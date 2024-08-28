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
package com.ericsson.component.aia.services.bps.spark.kafka;

import com.ericsson.component.aia.common.transport.kafka.writer.api.KafkaKeyGenerator;

/**
 * The Class KeyGeneratorUtil is used to convert a class name string into an instance of a KafkaKeyGenerator.
 */
public class KeyGeneratorUtil {

    private KeyGeneratorUtil() {

    }

    /**
     * Gets the key generator instance.
     *
     * @param <K>
     *            the key type
     * @param <V>
     *            the value type
     * @param keyGeneratorClazz
     *            the key generator clazz
     * @return the key generator instance
     */
    @SuppressWarnings("unchecked")
    public static <K, V> KafkaKeyGenerator<K, V> getKeyGeneratorInstance(final String keyGeneratorClazz) {
        if (keyGeneratorClazz != null) {
            try {
                final Object kafkaKeyGeneratorInstance = Class.forName(keyGeneratorClazz).newInstance();
                if (kafkaKeyGeneratorInstance instanceof KafkaKeyGenerator) {
                    return (KafkaKeyGenerator<K, V>) kafkaKeyGeneratorInstance;
                }

            } catch (final Exception exception) {
                throw new IllegalArgumentException("Unable to create an instance of KafkaKeyGenerator ", exception);
            }
        }
        return null;
    }

}
