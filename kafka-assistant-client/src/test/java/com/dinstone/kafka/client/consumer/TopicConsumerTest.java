
package com.dinstone.kafka.client.consumer;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dinstone.kafka.client.consumer.ConsumerConfig;
import com.dinstone.kafka.client.consumer.MessageHandler;
import com.dinstone.kafka.client.consumer.TopicConsumer;

public class TopicConsumerTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        MessageHandler<String, String> handleService = new MessageHandler<String, String>() {

            @Override
            public void handle(String key, String message) throws Exception {
                System.out.println("value = " + message);
                // Thread.sleep(800);
            }
        };

        ConsumerConfig consumeConfig = new ConsumerConfig("config-consumer-test.xml");
        TopicConsumer<String, String> process = new TopicConsumer<String, String>(consumeConfig, handleService);
        process.start();

        try {
            System.in.read();
        } catch (IOException e) {
        }

        process.stop();

        try {
            System.in.read();
        } catch (IOException e) {
        }
    }

}
