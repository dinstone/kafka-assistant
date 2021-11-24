
package com.dinstone.kafka.client.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.dinstone.kafka.client.ConsumerFactory;

public class TopicConsumer<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(TopicConsumer.class);

	private final String topicName;

	private ConsumerConfig consumerConfig;

	private MessageHandler<K, V> messageHandler;

	private ConsumerFactory<K, V> consumerFactory;

	private TopicConsumeRunner topicConsumeRunner;

	public TopicConsumer(ConsumerConfig consumerConfig, MessageHandler<K, V> messageHandler) {
		this(consumerConfig, messageHandler, null, null);
	}

	public TopicConsumer(ConsumerConfig consumerConfig, MessageHandler<K, V> messageHandler,
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
		this.topicConsumeRunner = new TopicConsumeRunner(consumerConfig);
	}

	public void start() {
		LOG.info("start topic[{}] consume process", consumerConfig.getTopic());
		topicConsumeRunner.start();
	}

	public void stop() {
		LOG.info("stop topic[{}] consume process", consumerConfig.getTopic());
		topicConsumeRunner.shutdown();
	}

	private class TopicConsumeRunner extends Thread {

		private final Map<TopicPartition, PartitionConsumer<K, V>> partitionConsumers = new HashMap<>();

		private final AtomicBoolean closed = new AtomicBoolean(false);

		private ConsumerRebalanceListener rebalanceListener;

		private Consumer<K, V> kafkaConsumer;

		private long pollTimeOut;

		public TopicConsumeRunner(ConsumerConfig consumerConfig) {
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

						// kafka comsumer is invalid, so must shutdown partition consumer.
						for (PartitionConsumer<K, V> partitionConsumer : partitionConsumers.values()) {
							partitionConsumer.shutdown();
						}
						partitionConsumers.clear();
					}
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					LOG.info("Assign: {}}", partitions);

					// create new partition consumers
					Map<TopicPartition, PartitionConsumer<K, V>> newConsumers = new HashMap<>();
					for (TopicPartition partition : partitions) {
						OffsetAndMetadata osm = kafkaConsumer.committed(partition);
						long cdOffset = osm == null ? -1 : osm.offset();
						long position = kafkaConsumer.position(partition);
						LOG.info("{} commited offset={}, position={}", partition, cdOffset, position);

						PartitionConsumer<K, V> consumer = partitionConsumers.get(partition);
						if (consumer == null) {
							newConsumers.put(partition,
									new PartitionConsumer<K, V>(partition, consumerConfig, messageHandler));
						} else {
							newConsumers.put(partition, consumer);
						}
					}

					// clear revoked partition consumers
					for (Iterator<Entry<TopicPartition, PartitionConsumer<K, V>>> iterator = partitionConsumers
							.entrySet().iterator(); iterator.hasNext();) {
						Entry<TopicPartition, PartitionConsumer<K, V>> tpce = iterator.next();
						if (!newConsumers.containsKey(tpce.getKey())) {
							// need to shutdown
							tpce.getValue().shutdown();
						}
						iterator.remove();
					}

					// add new partition consumers
					partitionConsumers.putAll(newConsumers);
				}
			};
		}

		public void run() {
			// init kafka consumer
			createKafkaConsumer();

			Duration ptoMillis = Duration.ofMillis(pollTimeOut);
			while (!closed.get()) {
				long stime = System.currentTimeMillis();
				try {
					ConsumerRecords<K, V> records = kafkaConsumer.poll(ptoMillis);
					LOG.debug("{} poll records size {}", topicName, records.count());

					// commit ack offset
					commitFinishOffset();

					// consume by partition consumer
					for (TopicPartition partition : records.partitions()) {
						List<ConsumerRecord<K, V>> recordList = records.records(partition);
						LOG.debug("{} poll records size {}", partition, recordList.size());
						PartitionConsumer<K, V> pc = partitionConsumers.get(partition);
						// submit records and control rate
						long submitOffset = pc.submit(recordList);
						if (submitOffset >= 0) {
							kafkaConsumer.seek(partition, submitOffset + 1);
						}
					}
				} catch (Exception e) {
					// Ignore exception if closing
					if (!closed.get()) {
						LOG.warn("create a new KafkaConsumer for topic {} by error : {}", topicName, e.getMessage());
						closeKafkaConsumer();
						createKafkaConsumer();
					}
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

		private void commitFinishOffset() {
			partitionConsumers.forEach((partition, consumer) -> {
				// find finish offset
				long finishOffset = consumer.finish();
				if (finishOffset > -1) {
					// sync ack commit offset
					OffsetAndMetadata cos = new OffsetAndMetadata(finishOffset + 1);
					kafkaConsumer.commitSync(Collections.singletonMap(partition, cos));
				}
			});
		}

		private void createKafkaConsumer() {
			kafkaConsumer = consumerFactory.createConsumer();
			kafkaConsumer.subscribe(Arrays.asList(topicName), rebalanceListener);
		}

		private void closeKafkaConsumer() {
			if (kafkaConsumer != null) {
				kafkaConsumer.unsubscribe();
				kafkaConsumer.close();
			}

			// shutdown and clear partition consumer
			partitionConsumers.forEach((tp, pc) -> {
				pc.shutdown();
			});
			partitionConsumers.clear();
		}

		// Shutdown hook which can be called from a separate thread
		private void shutdown() {
			closed.set(true);
			if (kafkaConsumer != null) {
				kafkaConsumer.wakeup();
			}
		}
	}

}
