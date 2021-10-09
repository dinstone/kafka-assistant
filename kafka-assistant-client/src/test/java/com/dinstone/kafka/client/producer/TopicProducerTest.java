
package com.dinstone.kafka.client.producer;

public class TopicProducerTest {

    public static void main(String[] args) {
        ProducerConfig config = new ProducerConfig("config-producer-test.xml");
        TopicProducer<String, String> producer = new TopicProducer<String, String>(config);
        long st = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            producer.send("" + i, "a" + i);
        }

        producer.flush();
        long et = System.currentTimeMillis();

        System.out.println("ok, take's " + (et - st) + ", " + 10000 * 1000 / (et - st) + "tps");
        producer.destroy();
    }

}
