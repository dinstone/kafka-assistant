
package com.dinstone.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler<K, V> {

	public void handle(ConsumerRecord<K, V> consumerRecord) throws Exception;

}
