
package com.dinstone.kafka.client.producer;

import java.util.Properties;

import com.dinstone.kafka.client.Configuration;

public class ProducerConfig extends Configuration {

    public ProducerConfig() {
        super();
    }

    public ProducerConfig(Configuration config) {
        super(config);
    }

    public ProducerConfig(String configLocation) {
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
