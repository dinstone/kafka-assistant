
package com.dinstone.kafka.client.consumer;

public interface MessageHandler<K, V> {

    public void handle(K key, V message) throws Exception;

}
