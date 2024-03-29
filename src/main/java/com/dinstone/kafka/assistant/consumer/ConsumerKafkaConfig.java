package com.dinstone.kafka.assistant.consumer;

import com.dinstone.kafka.assistant.KafkaConfig;

import java.util.Properties;

public class ConsumerKafkaConfig extends KafkaConfig {

    public ConsumerKafkaConfig() {
        super();
    }

    public ConsumerKafkaConfig(KafkaConfig config) {
        super(config);
    }

    public ConsumerKafkaConfig(String configLocation) {
        super(configLocation);
    }

    public Properties getKafkaProperties() {
        Properties ret = new Properties();
        ret.putAll(properties);
        ret.remove("kafka.topic");
        ret.remove("poll.timeout.ms");
        ret.remove("topic.handler.count");
        ret.remove("message.queue.size");
        ret.remove("parallel.consumer.size");
        return ret;
    }

    public boolean getAutoCommit() {
        return getBoolean("enable.auto.commit", true);
    }

    public void setAutoCommit(boolean auto) {
        set("enable.auto.commit", auto ? "true" : "false");
    }

    public long getPollTimeOut() {
        return getLong("poll.timeout.ms", 10 * 1000);
    }

    public String getTopic() {
        return get("kafka.topic");
    }

    public void setTopic(String topic) {
        set("kafka.topic", topic);
    }

    public int getHandlerCount() {
        return getInt("topic.handler.count", Runtime.getRuntime().availableProcessors());
    }

    public void setHandlerCount(int count) {
        setInt("topic.handler.count", count);
    }

    public int getMessageQueueSize() {
        return getInt("message.queue.size", 15);
    }

    public void setMessageQueueSize(int size) {
        setInt("message.queue.size", size);
    }

    public int getParallelConsumerSize() {
        return getInt("parallel.consumer.size", 3);
    }

    public void setParallelConsumerSize(int size) {
        setInt("parallel.consumer.size", size);
    }

}
