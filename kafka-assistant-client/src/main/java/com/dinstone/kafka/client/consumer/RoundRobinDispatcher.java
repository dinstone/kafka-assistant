
package com.dinstone.kafka.client.consumer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RoundRobinDispatcher implements RecordDispatcher {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    @Override
    public int dispatch(ConsumerRecord<?, ?> record, int runnerSize) {
        if (runnerSize == 0) {
            return -1;
        }

        int nextValue = counter.getAndIncrement();
        return toPositive(nextValue) % runnerSize;
    }

    private int toPositive(int number) {
        return number & 0x7fffffff;
    }

}
