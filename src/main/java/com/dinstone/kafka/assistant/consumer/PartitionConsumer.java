
package com.dinstone.kafka.assistant.consumer;

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

    private final BlockingQueue<ConsumerTask<K, V>> submitQueue = new LinkedBlockingQueue<>();

    private final Queue<ConsumerTask<K, V>> futureQueue = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final MessageHandler<K, V> messageHandler;

    private final ExecutorService executor;

    private final TopicPartition partition;

    private final int messageQueueSize;

    private long submitOffset = -1;

    private long finishOffset = -1;

    public PartitionConsumer(TopicPartition partition, ConsumerKafkaConfig consumerConfig,
            MessageHandler<K, V> messageHandler) {
        this.partition = partition;
        this.messageHandler = messageHandler;

        this.messageQueueSize = consumerConfig.getMessageQueueSize();

        int parallelSize = consumerConfig.getParallelConsumerSize();
        if (parallelSize <= 0) {
            throw new IllegalArgumentException("partition parallel.consumer.size must bigger than 0");
        }

        this.executor = Executors.newCachedThreadPool();
        for (int i = 0; i < parallelSize; i++) {
            this.executor.execute(new PartitionEventLoop(i));
        }
    }

    public long submitOffset() {
        return submitOffset;
    }

    public long finishOffset() {
        return finishOffset;
    }

    public boolean isFull() {
        return futureQueue.size() >= messageQueueSize;
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
        ConsumerTask<K, V> last = null;
        for (ConsumerRecord<K, V> record : recordList) {
            if (!isFull()) {
                last = new ConsumerTask<K, V>(record);
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
        return count;
    }

    /**
     * find last finish offset
     * 
     * @return offset first finish
     */
    public long finish() {
        int count = 0;
        ConsumerTask<K, V> last = null;
        for (;;) {
            ConsumerTask<K, V> check = futureQueue.peek();
            if (check == null || !check.isFinish()) {
                break;
            }

            last = futureQueue.poll();
            //
            count++;
        }
        if (last != null) {
            finishOffset = last.record().offset();
        }

        LOG.debug("{} finish count {}, last offset {}", partition, count, finishOffset);
        return count;
    }

    public void shutdown() {
        shutdown.set(true);
        executor.shutdownNow();
        LOG.info("{} consumer shutdown, submit/future: {}/{} tasks untreated, submit/finish: {}/{} offset", partition,
            submitQueue.size(), futureQueue.size(), submitOffset, finishOffset);
    }

    @Override
    public String toString() {
        return "PartitionConsumer [partition=" + partition + ", submitOffset=" + submitOffset + ", finishOffset="
                + finishOffset + "]";
    }

    private class PartitionEventLoop implements Runnable {

        private String tname;

        public PartitionEventLoop(int index) {
            this.tname = "Partition[" + partition + "]-Work-" + index;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(tname);

            ConsumerTask<K, V> task = null;
            while (!shutdown.get() && !Thread.interrupted()) {
                try {
                    task = submitQueue.take();

                    messageHandler.handle(task.schedule());

                    task.complete();
                } catch (Throwable e) {
                    // InterruptedException break, other ignore
                    if (e instanceof InterruptedException) {
                        break;
                    }

                    if (task != null && !task.isFinish()) {
                        task.complete(e);
                    }
                }
            }
        }

    }

}
