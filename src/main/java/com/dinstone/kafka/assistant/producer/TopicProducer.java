package com.dinstone.kafka.assistant.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.concurrent.Future;

public class TopicProducer<K, V> {

    private String topic;

    private Producer<K, V> kafkaProducer;

    private ProducerFactory<K, V> producerFactory;

    public TopicProducer(ProducerKafkaConfig config) {
        this(config, null, null, null);
    }

    public TopicProducer(ProducerKafkaConfig config, Callback defaultCallback) {
        this(config, defaultCallback, null, null);
    }

    public TopicProducer(ProducerKafkaConfig config, Callback defaultCallback, Serializer<K> keySerializer,
                         Serializer<V> valueSerializer) {
        if (config == null) {
            throw new IllegalArgumentException("config is null");
        }

        if (config.getTopic() == null || config.getTopic().length() == 0) {
            throw new IllegalArgumentException("kafka.topic is empty");
        }
        this.topic = config.getTopic();

        this.producerFactory = new ProducerFactory<K, V>(config.getKafkaConfig(), keySerializer, valueSerializer,
                defaultCallback);
    }

    public Future<RecordMetadata> send(K key, V data) {
        synchronized (this) {
            if (this.kafkaProducer == null) {
                this.kafkaProducer = this.producerFactory.createProducer();
            }
        }

        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, data);
        return this.kafkaProducer.send(record);
    }

    public Future<RecordMetadata> send(K key, V data, Callback callback) {
        synchronized (this) {
            if (this.kafkaProducer == null) {
                this.kafkaProducer = this.producerFactory.createProducer();
            }
        }

        ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, data);
        return this.kafkaProducer.send(record, callback);
    }

    public void flush() {
        synchronized (this) {
            if (this.kafkaProducer != null) {
                this.kafkaProducer.flush();
            }
        }
    }

    public void destroy() {
        synchronized (this) {
            if (this.kafkaProducer != null) {
                this.kafkaProducer.close();
            }
        }
    }
}
