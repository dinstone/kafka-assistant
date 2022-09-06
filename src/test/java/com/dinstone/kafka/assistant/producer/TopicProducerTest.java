package com.dinstone.kafka.assistant.producer;

public class TopicProducerTest {

    public static void main(String[] args) {
        ProducerKafkaConfig config = new ProducerKafkaConfig("config-producer-test.xml");
        TopicProducer<String, String> producer = new TopicProducer<String, String>(config);

        long st = System.currentTimeMillis();
        int count = 100;
        for (int i = 0; i < count; i++) {
            producer.send("" + i, "a" + i);
        }

        producer.flush();
        long et = System.currentTimeMillis();

        System.out.println("ok," + count + " take's " + (et - st) + ", " + count * 1000 / (et - st) + "tps");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        producer.destroy();
    }

}
