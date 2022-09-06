package com.dinstone.kafka.assistant.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ProducerFactory<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerFactory.class);

    private final Properties configs = new Properties();

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;

    private final Callback defaultCallback;

    public ProducerFactory(Properties configs) {
        this(configs, null, null, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.error("when the record sent to the server has been acknowledged", exception);
                }
            }

        });
    }

    public ProducerFactory(Properties configs, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                           Callback defaultCallback) {
        if (configs == null) {
            throw new IllegalArgumentException("configs is null");
        }
        this.configs.putAll(configs);

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.defaultCallback = defaultCallback;
    }

    public Producer<K, V> createProducer() {
        return new CallbackProducer<K, V>(new KafkaProducer<K, V>(this.configs, keySerializer, valueSerializer),
                defaultCallback);
    }

    private static class CallbackProducer<K, V> implements Producer<K, V> {

        private final Producer<K, V> delegate;

        private Callback defaultCallback;

        public CallbackProducer(Producer<K, V> delegate, Callback defaultCallback) {
            this.delegate = delegate;
            this.defaultCallback = defaultCallback;
        }

        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return this.delegate.send(record, defaultCallback);
        }

        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return this.delegate.send(record, callback);
        }

        public void flush() {
            this.delegate.flush();
        }

        public List<PartitionInfo> partitionsFor(String topic) {
            return this.delegate.partitionsFor(topic);
        }

        public Map<MetricName, ? extends Metric> metrics() {
            return this.delegate.metrics();
        }

        public void close() {
            this.delegate.close();
        }

        public void close(long timeout, TimeUnit unit) {
            this.delegate.close(timeout, unit);
        }

        @Override
        public void initTransactions() {
            this.delegate.initTransactions();
        }

        @Override
        public void beginTransaction() throws ProducerFencedException {
            this.delegate.beginTransaction();
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
                throws ProducerFencedException {
            this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            this.delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() throws ProducerFencedException {
            this.delegate.abortTransaction();
        }

    }

}
