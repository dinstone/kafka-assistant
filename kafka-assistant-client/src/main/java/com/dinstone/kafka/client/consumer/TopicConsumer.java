
package com.dinstone.kafka.client.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dinstone.kafka.client.ConsumerFactory;

public class TopicConsumer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(TopicConsumer.class);

    private ConsumerConfig consumerConfig;

    private MessageHandler<K, V> messageHandler;

    private ConsumerFactory<K, V> consumerFactory;

    private TopicConsumeRunner topicConsumeRunner;

    private List<RecordConsumeRunner> recordConsumeRunners;

    private RecordDispatcher recordDispatcher = new KeyStickDispatcher();

    public TopicConsumer(ConsumerConfig consumerConfig, MessageHandler<K, V> messageHandler) {
        this(consumerConfig, messageHandler, null, null, null);
    }

    public TopicConsumer(ConsumerConfig consumerConfig, MessageHandler<K, V> messageHandler, RecordDispatcher dispatcher) {
        this(consumerConfig, messageHandler, dispatcher, null, null);
    }

    public TopicConsumer(ConsumerConfig consumerConfig, MessageHandler<K, V> messageHandler,
            RecordDispatcher dispatcher, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        String topic = consumerConfig.getTopic();
        if (topic == null || topic.length() == 0) {
            throw new IllegalArgumentException("kafka.topic is empty");
        }
        this.consumerConfig = consumerConfig;

        if (messageHandler == null) {
            throw new IllegalArgumentException("messageHandler is null");
        }
        this.messageHandler = messageHandler;

        if (dispatcher != null) {
            this.recordDispatcher = dispatcher;
        }

        // create topic consumer runner
        this.topicConsumeRunner = new TopicConsumeRunner(consumerConfig);

        this.consumerFactory = new ConsumerFactory<K, V>(consumerConfig.getKafkaConfig(), keyDeserializer,
            valueDeserializer);

        this.recordConsumeRunners = new ArrayList<RecordConsumeRunner>();
        for (int j = 0; j < consumerConfig.getHandlerCount(); j++) {
            this.recordConsumeRunners.add(new RecordConsumeRunner(j, consumerConfig));
        }
    }

    public void start() {
        LOG.info("start topic[{}] consume process", consumerConfig.getTopic());
        topicConsumeRunner.start();

        for (RecordConsumeRunner messageConsumeRunner : recordConsumeRunners) {
            messageConsumeRunner.start();
        }
    }

    public void stop() {
        LOG.info("stop topic[{}] consume process", consumerConfig.getTopic());
        topicConsumeRunner.shutdown();

        for (RecordConsumeRunner messageConsumeRunner : recordConsumeRunners) {
            messageConsumeRunner.shutdown();
        }
    }

    private void dispatch(TopicPartition partition, ConsumerRecord<K, V> record) {
        try {
            if (record.key() == null || record.value() == null) {
                return;
            }

            int index = recordDispatcher.dispatch(record, recordConsumeRunners.size());
            if (index > -1 && index < recordConsumeRunners.size()) {
                recordConsumeRunners.get(index).submit(record);
            }
        } catch (Exception e) {
            LOG.error("process [{} : {}], error : {}", partition, record.key(), e.getMessage());
        }
    }

    private void handle(ConsumerRecord<K, V> record) {
        try {
            messageHandler.handle(record.key(), record.value());
        } catch (Exception e) {
            LOG.warn("handle {}, error : {}", record, e.getMessage());
        }
    }

    private class TopicConsumeRunner extends Thread {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ConsumerConfig consumerConfig;

        private long pollTimeOut;

        private int commitBatchSize;

        private Consumer<K, V> kafkaConsumer;

        public TopicConsumeRunner(ConsumerConfig consumerConfig) {
            this.consumerConfig = consumerConfig;
            this.pollTimeOut = consumerConfig.getPollTimeOut();
            this.commitBatchSize = consumerConfig.getCommitBatchSize();
            setName("Topic[" + consumerConfig.getTopic() + "]");
        }

        public void run() {
            boolean autoCommit = consumerConfig.getAutoCommit();

            createKafkaConsumer();
            while (!closed.get()) {
                try {
                    ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeOut);
                    for (TopicPartition partition : records.partitions()) {
                        int count = 0;
                        List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<K, V> record : partitionRecords) {
                            dispatch(partition, record);

                            count++;
                            if (!autoCommit && (count % commitBatchSize == 0)) {
                                long lastOffset = partitionRecords.get(count - 1).offset();
                                kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(
                                    lastOffset + 1)));
                            }
                        }

                        if (!autoCommit && count > 0) {
                            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                            kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(
                                lastOffset + 1)));
                        }
                    }
                } catch (Exception e) {
                    // Ignore exception if closing
                    if (!closed.get()) {
                        LOG.warn("create a new KafkaConsumer for topic {} by error : {}", consumerConfig.getTopic(),
                            e.getMessage());
                        closeKafkaConsumer();
                        createKafkaConsumer();
                    }
                }
            }

            closeKafkaConsumer();
        }

        private void createKafkaConsumer() {
            kafkaConsumer = consumerFactory.createConsumer();

            kafkaConsumer.subscribe(Arrays.asList(consumerConfig.getTopic()));
        }

        private void closeKafkaConsumer() {
            if (kafkaConsumer != null) {
                kafkaConsumer.unsubscribe();
                kafkaConsumer.close();
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            if (kafkaConsumer != null) {
                kafkaConsumer.wakeup();
            }
        }
    }

    private class RecordConsumeRunner extends Thread {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private BlockingQueue<ConsumerRecord<K, V>> recordQueue;

        public RecordConsumeRunner(int index, ConsumerConfig consumerConfig) {
            this.recordQueue = new LinkedBlockingQueue<ConsumerRecord<K, V>>(consumerConfig.getMessageQueueSize());
            setName("Topic[" + consumerConfig.getTopic() + "]-Record-" + index);
        }

        public void shutdown() {
            this.closed.set(true);
            this.interrupt();
        }

        public void submit(ConsumerRecord<K, V> record) {
            try {
                recordQueue.put(record);
            } catch (InterruptedException e) {
                LOG.warn("current thread is be interrupted");
            }
        }

        @Override
        public void run() {
            while (!closed.get()) {
                try {
                    ConsumerRecord<K, V> record = recordQueue.take();

                    handle(record);
                } catch (InterruptedException e) {
                    if (!closed.get()) {
                        LOG.warn("current thread is be interrupted,but not close singal.");
                    }
                }
            }

            LOG.warn("remaining {} records untreated", recordQueue.size());
        }

    }

}
