package com.dinstone.kafka.client.consumer;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionConsumer<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionConsumer.class);

	private final BlockingQueue<RecordFuture<K, V>> submitQueue = new LinkedBlockingQueue<>();
	private final Queue<RecordFuture<K, V>> futureQueue = new ConcurrentLinkedQueue<>();
	private final AtomicBoolean shutdown = new AtomicBoolean(false);

	private final MessageHandler<K, V> messageHandler;

	private final ExecutorService executor;

	private final TopicPartition partition;

	private final int messageQueueSize;

	private long submitOffset = -1;

	public PartitionConsumer(TopicPartition partition, ConsumerConfig consumerConfig,
			MessageHandler<K, V> messageHandler) {
		this.partition = partition;
		this.messageHandler = messageHandler;

		this.messageQueueSize = consumerConfig.getMessageQueueSize();

		int parallelSize = consumerConfig.getParallelConsumerSize();
		this.executor = Executors.newCachedThreadPool();
		for (int i = 0; i < parallelSize; i++) {
			this.executor.execute(new RecordConsumeRunner(i));
		}
	}

	/**
	 * submit record to consume
	 * 
	 * @param <K>
	 * @param <V>
	 * @param recordList
	 * @return offset last submit
	 */
	public long submit(List<ConsumerRecord<K, V>> recordList) {
		int count = 0;
		RecordFuture<K, V> last = null;
		for (ConsumerRecord<K, V> record : recordList) {
			if (futureQueue.size() < messageQueueSize) {
				last = new RecordFuture<K, V>(record);
				futureQueue.add(last);
				submitQueue.add(last);
				//
				count++;
			}
		}
		if (last != null) {
			submitOffset = last.record().offset();
		}
		LOG.debug("{} submit count {}, last offset {}", partition, count, submitOffset);
		return submitOffset;
	}

	/**
	 * find consuming finish offset
	 * 
	 * @return offset first finish
	 */
	public long finish() {
		int count = 0;
		RecordFuture<K, V> last = null;
		for (;;) {
			last = futureQueue.peek();
			if (last == null || !last.isComplete()) {
				break;
			}

			futureQueue.poll();
			//
			count++;
		}

		long offset = last == null ? 0 : last.record().offset();
		LOG.debug("{} finish count {}, last offset {}", partition, count, offset);
		return offset;
	}

	public void shutdown() {
		LOG.info("{} will shutdown", partition);
		shutdown.set(true);
		executor.shutdownNow();
	}

	private class RecordConsumeRunner implements Runnable {

		private String tname;

		public RecordConsumeRunner(int index) {
			this.tname = "Partition[" + partition + "]-Work-" + index;
		}

		@Override
		public void run() {
			Thread.currentThread().setName(tname);

			RecordFuture<K, V> record = null;
			while (!shutdown.get() && !Thread.interrupted()) {
				try {
					record = submitQueue.take();

					messageHandler.handle(record.schedule());

					record.complete();
					record = null;
				} catch (Throwable e) {
					if (record != null) {
						record.complete(e);
					}

					// InterruptedException break, other ignore
					if (e instanceof InterruptedException) {
						break;
					}
				}
			}

			LOG.warn("submit/future : {}/{} records untreated", submitQueue.size(), futureQueue.size());
		}

	}

}
