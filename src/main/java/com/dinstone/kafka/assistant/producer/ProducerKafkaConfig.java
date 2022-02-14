
package com.dinstone.kafka.assistant.producer;

import java.util.Properties;

import com.dinstone.kafka.assistant.KafkaConfig;

public class ProducerKafkaConfig extends KafkaConfig {

    public ProducerKafkaConfig() {
        super();
    }

    public ProducerKafkaConfig(KafkaConfig config) {
        super(config);
    }

    public ProducerKafkaConfig(String configLocation) {
        super(configLocation);
    }

    public Properties getKafkaConfig() {
        Properties ret = new Properties();
        ret.putAll(properties);
        ret.remove("kafka.topic");
        return ret;
    }

    public String getTopic() {
        return get("kafka.topic");
    }

    public void setTopic(String topic) {
        set("kafka.topic", topic);
    }

}
