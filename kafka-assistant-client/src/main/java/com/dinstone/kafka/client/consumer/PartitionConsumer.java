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
		ConsumerRecord<K, V> last = null;
		for (ConsumerRecord<K, V> record : recordList) {
			if (futureQueue.size() < messageQueueSize) {
				RecordFuture<K, V> pr = new RecordFuture<K, V>(record);
				futureQueue.add(pr);
				submitQueue.add(pr);
				last = record;
				//
				count++;
			}
		}
		if (last != null) {
			submitOffset = last.offset();
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
		ConsumerRecord<?, ?> last = null;
		for (;;) {
			RecordFuture<K, V> pr = futureQueue.peek();
			if (pr == null || !pr.isComplete()) {
				break;
			}

			futureQueue.poll();
			last = pr.getRecord();
			//
			count++;
		}

		long offset = last == null ? 0 : last.offset();
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

					messageHandler.handle(record.getRecord());

					record.complete(1);
					record = null;
				} catch (Throwable e) {
					if (record != null) {
						record.complete(-1);
					}

					// InterruptedException break, other ignore
					if (e instanceof InterruptedException) {
						break;
					}
				}
			}

			LOG.warn("submit/finish : {}/{} records untreated", submitQueue.size(), futureQueue.size());
		}

	}

}
