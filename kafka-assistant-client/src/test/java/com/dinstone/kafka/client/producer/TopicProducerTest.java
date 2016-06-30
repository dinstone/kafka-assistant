
package com.dinstone.kafka.client.producer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dinstone.kafka.client.producer.ProducerConfig;
import com.dinstone.kafka.client.producer.TopicProducer;

public class TopicProducerTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSendKVCallback() {
        ProducerConfig config = new ProducerConfig("config-producer-test.xml");
        TopicProducer<String, String> producer = new TopicProducer<String, String>(config);
        long st = System.currentTimeMillis();
        byte[] data = new byte[1024];
        for (int i = 0; i < 10000; i++) {
            producer.send("" + i, new String(data));
        }

        producer.flush();
        long et = System.currentTimeMillis();

        System.out.println("ok, take's " + (et - st) + ", " + 10000 * 1000 / (et - st) + "tps");
        producer.destroy();
    }

}
