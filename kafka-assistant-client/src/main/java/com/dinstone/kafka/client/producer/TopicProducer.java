
package com.dinstone.kafka.client.producer;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import com.dinstone.kafka.client.ProducerFactory;

public class TopicProducer<K, V> {

	private String topic;

	private Producer<K, V> kafkaProducer;

	private ProducerFactory<K, V> producerFactory;

	public TopicProducer(ProducerConfig config) {
		this(config, null, null, null, null);
	}

	public TopicProducer(ProducerConfig config, String topic) {
		this(config, topic, null, null, null);
	}

	public TopicProducer(ProducerConfig config, String topic, Serializer<K> keySerializer,
			Serializer<V> valueSerializer, Callback defaultCallback) {
		if (config == null) {
			throw new IllegalArgumentException("config is null");
		}

		if (topic != null && !topic.isEmpty()) {
			this.topic = topic;
		} else {
			if (config.getTopic() == null || config.getTopic().length() == 0) {
				throw new IllegalArgumentException("kafka.topic is empty");
			}
			this.topic = config.getTopic();
		}

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
