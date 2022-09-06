# kafka assistant

# 1.Install kafka

## with a docker compose file

```
> cd kafka-docker
> docker-compose up -d
```

## produce test

```
> docker exec -it kafka_kafkaNode_1 sh
> kafka-console-producer.sh --broker-list localhost:9092 --topic kafeidou
>> hello world
```

## consume test

```
> docker exec -it kafka_kafkaNode_1 sh
> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafeidou --from-beginning
```

# 2.API access kafka

## produce message to kafka topic

```
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
```

## parallel consume message from kafka topic

```
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
        // consumeConfig.setParallelConsumerSize(1);
        // consumeConfig.setMessageQueueSize(3);
        TopicConsumer<String, String> process = new TopicConsumer<String, String>(consumeConfig, handleService);
        process.start();

        try {
            System.in.read();
        } catch (IOException e) {
        }

        process.stop();
    }

}
```
