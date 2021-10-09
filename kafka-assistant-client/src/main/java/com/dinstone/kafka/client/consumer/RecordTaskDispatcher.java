
package com.dinstone.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordTaskDispatcher {

    int dispatch(ConsumerRecord<?, ?> record, int runnerSize);

}
