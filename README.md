# kafka assistant

# 1.Install kafka 
## with docker compse file
```
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
	}
}
```