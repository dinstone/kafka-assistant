
package com.dinstone.kafka.assistant.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicConsumer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(TopicConsumer.class);

    private final String topicName;

    private ConsumerKafkaConfig consumerConfig;

    private MessageHandler<K, V> messageHandler;

    private ConsumerFactory<K, V> consumerFactory;

    private TopicEventLoop topicEventLoop;

    public TopicConsumer(ConsumerKafkaConfig consumerConfig, MessageHandler<K, V> messageHandler) {
        this(consumerConfig, messageHandler, null, null);
    }

    public TopicConsumer(ConsumerKafkaConfig consumerConfig, MessageHandler<K, V> messageHandler,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        String topic = consumerConfig.getTopic();
        if (topic == null || topic.length() == 0) {
            throw new IllegalArgumentException("kafka.topic is empty");
        }
        this.consumerConfig = consumerConfig;
        this.topicName = consumerConfig.getTopic();

        if (messageHandler == null) {
            throw new IllegalArgumentException("messageHandler is null");
        }
        this.messageHandler = messageHandler;

        // set auto commit = false
        consumerConfig.setAutoCommit(false);

        // create kafka consumer factory
        this.consumerFactory = new ConsumerFactory<K, V>(consumerConfig.getKafkaConfig(), keyDeserializer,
            valueDeserializer);

        // create topic consumer runner
        this.topicEventLoop = new TopicEventLoop(consumerConfig);
    }

    public void start() {
        LOG.info("start topic[{}] consume process", consumerConfig.getTopic());
        topicEventLoop.start();
    }

    public void stop() {
        LOG.info("stop topic[{}] consume process", consumerConfig.getTopic());
        topicEventLoop.close();
    }

    private class TopicEventLoop extends Thread {

        private final Map<TopicPartition, PartitionConsumer<K, V>> partitionConsumers = new HashMap<>();

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ConsumerRebalanceListener rebalanceListener;

        private Consumer<K, V> kafkaConsumer;

        private long pollTimeOut;

        public TopicEventLoop(ConsumerKafkaConfig consumerConfig) {
            this.pollTimeOut = consumerConfig.getPollTimeOut();
            // Thread name
            setName("Topic[" + consumerConfig.getTopic() + "]-Poller");

            this.rebalanceListener = new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    LOG.info("Revoke: {}}", partitions);
                    try {
                        commitFinishOffset();
                    } catch (Exception e) {
                        LOG.warn("Revoke commit offset error {}", e.getMessage());

                        // kafka comsumer is invalid, so must close partition consumer.
                        closePartitionConsumers();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    LOG.info("Assign: {}}", partitions);

                    // resume consuming
                    kafkaConsumer.resume(partitions);

                    // create new partition consumers
                    Map<TopicPartition, PartitionConsumer<K, V>> newConsumers = new HashMap<>();
                    for (TopicPartition partition : partitions) {
                        PartitionConsumer<K, V> consumer = partitionConsumers.get(partition);
                        if (consumer == null) {
                            newConsumers.put(partition,
                                new PartitionConsumer<K, V>(partition, consumerConfig, messageHandler));
                        } else {
                            newConsumers.put(partition, consumer);
                        }
                    }
                    // clear revoked partition consumers
                    partitionConsumers.forEach((partition, consumer) -> {
                        if (!newConsumers.containsKey(partition)) {
                            // need to shutdown
                            consumer.shutdown();
                        }
                    });
                    partitionConsumers.clear();
                    // add new partition consumers
                    partitionConsumers.putAll(newConsumers);

                    // show partition state info
                    partitionConsumers.forEach((partition, consumer) -> {
                        OffsetAndMetadata osm = kafkaConsumer.committed(partition);
                        long committed = osm == null ? -1 : osm.offset();
                        long position = kafkaConsumer.position(partition);
                        long submit = consumer.submitOffset();
                        long finish = consumer.finishOffset();
                        LOG.info("{} commited={}, position={} ; submit={}, finish={}", partition, committed, position,
                            submit, finish);
                    });
                }
            };
        }

        public void run() {
            Duration ptoMillis = Duration.ofMillis(pollTimeOut);
            while (!closed.get()) {
                // init kafka consumer
                createKafkaConsumer();

                long stime = System.currentTimeMillis();
                try {
                    ConsumerRecords<K, V> records = kafkaConsumer.poll(ptoMillis);
                    LOG.debug("{} poll records size {}", topicName, records.count());

                    // commit finish offset
                    commitFinishOffset();

                    // dispatch records to partition consumer
                    dispatchRecords(records);

                    // control partition consume status
                    controlConsumeState();
                } catch (Exception e) {
                    // Ignore exception if closing
                    if (closed.get()) {
                        continue;
                    }

                    LOG.warn("create a new KafkaConsumer for topic {} by error : {}", topicName, e.getMessage());
                    closeKafkaConsumer();
                }
                long etime = System.currentTimeMillis();
                if (etime - stime < pollTimeOut) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            closeKafkaConsumer();
        }

        private void controlConsumeState() {
            partitionConsumers.forEach((partition, consumer) -> {
                // full check and pause or resume consuming
                if (consumer.isFull()) {
                    kafkaConsumer.pause(Collections.singleton(partition));
                } else {
                    kafkaConsumer.resume(Collections.singleton(partition));
                }
            });
        }

        private void dispatchRecords(ConsumerRecords<K, V> records) {
            // consume by partition consumer
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<K, V>> recordList = records.records(partition);
                LOG.debug("{} poll records size {}", partition, recordList.size());
                PartitionConsumer<K, V> pc = partitionConsumers.get(partition);
                // submit records and control rate
                long count = pc.submit(recordList);
                if (count > 0) {
                    kafkaConsumer.seek(partition, pc.submitOffset() + 1);
                }
            }
        }

        private void commitFinishOffset() {
            partitionConsumers.forEach((partition, consumer) -> {
                // process finish offset
                long count = consumer.finish();
                if (count > 0) {
                    // sync ack commit offset
                    long offset = consumer.finishOffset() + 1;
                    OffsetAndMetadata cos = new OffsetAndMetadata(offset);
                    kafkaConsumer.commitSync(Collections.singletonMap(partition, cos));
                }
            });
        }

        private void createKafkaConsumer() {
            if (kafkaConsumer == null) {
                kafkaConsumer = consumerFactory.createConsumer();
                kafkaConsumer.subscribe(Arrays.asList(topicName), rebalanceListener);
            }
        }

        private void closeKafkaConsumer() {
            if (kafkaConsumer != null) {
                kafkaConsumer.unsubscribe();
                kafkaConsumer.close();
                kafkaConsumer = null;
            }

            // shutdown and clear partition consumer
            closePartitionConsumers();
        }

        private void closePartitionConsumers() {
            partitionConsumers.forEach((tp, pc) -> {
                pc.shutdown();
            });
            partitionConsumers.clear();
        }

        // Shutdown hook which can be called from a separate thread
        private void close() {
            closed.set(true);
            if (kafkaConsumer != null) {
                kafkaConsumer.wakeup();
            }
        }
    }

}
