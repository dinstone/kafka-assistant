
package com.dinstone.kafka.assistant.consumer;

import java.io.IOException;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicConsumerTest {

	private static final Logger LOG = LoggerFactory.getLogger(TopicConsumerTest.class);

	public static void main(String[] args) {
		MessageHandler<String, String> handleService = new MessageHandler<String, String>() {

			@Override
			public void handle(ConsumerRecord<String, String> consumerRecord) throws Exception {
				Thread.sleep(new Random().nextInt(10) * 1000);
				LOG.error("{}-{} record: {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.key());
			}

		};

		ConsumerKafkaConfig consumeConfig = new ConsumerKafkaConfig("config-consumer-test.xml");
		TopicConsumer<String, String> process = new TopicConsumer<String, String>(consumeConfig, handleService);
		process.start();

		try {
			System.in.read();
		} catch (IOException e) {
		}

		process.stop();
	}

}
