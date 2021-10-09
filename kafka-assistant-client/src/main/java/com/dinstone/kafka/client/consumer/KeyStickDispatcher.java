
package com.dinstone.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KeyStickDispatcher implements RecordTaskDispatcher {

    @Override
    public int dispatch(ConsumerRecord<?, ?> record, int runnerSize) {
        if (runnerSize == 0) {
            return -1;
        }

        return toPositive(record.key().hashCode()) % runnerSize;
    }

    private int toPositive(int number) {
        return number & 0x7fffffff;
    }
}
