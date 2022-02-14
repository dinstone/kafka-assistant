
package com.dinstone.kafka.assistant.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

public class ConsumerFactory<K, V> {

    private final Properties configs = new Properties();

    private Deserializer<K> keyDeserializer;

    private Deserializer<V> valueDeserializer;

    public ConsumerFactory(Properties configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        if (configs == null) {
            throw new IllegalArgumentException("configs is null");
        }
        this.configs.putAll(configs);

        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public Consumer<K, V> createConsumer() {
        return new KafkaConsumer<>(this.configs, keyDeserializer, valueDeserializer);
    }

    public boolean isAutoCommit() {
        Object auto = this.configs.get("enable.auto.commit");
        return auto instanceof Boolean ? (Boolean) auto : auto instanceof String ? Boolean.valueOf((String) auto)
                : false;
    }

}
