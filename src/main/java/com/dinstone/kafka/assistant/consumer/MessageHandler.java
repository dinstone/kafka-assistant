
package com.dinstone.kafka.assistant.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler<K, V> {

	public void handle(ConsumerRecord<K, V> consumerRecord) throws Exception;

}
