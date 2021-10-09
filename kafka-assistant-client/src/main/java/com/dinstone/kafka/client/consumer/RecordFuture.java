package com.dinstone.kafka.client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RecordFuture<K, V> {

	private int complete;

	private ConsumerRecord<K, V> record;

	public RecordFuture(ConsumerRecord<K, V> record) {
		this.record = record;
	}

	public void complete(int complete) {
		this.complete = complete;
	}

	public boolean isComplete() {
		return complete != 0;
	}

	public ConsumerRecord<K, V> getRecord() {
		return record;
	}

	@Override
	public String toString() {
		return record == null ? "CloseRecord" : record.toString();
	}

}
