# install kafka with docker compose file

```
> docker-compose up -d
```

# produce test

```
> docker exec -it kafka_kafkaNode_1 sh
> kafka-console-producer.sh --broker-list localhost:9092 --topic kafeidou
>> hello world
```

# consume test

```
> docker exec -it kafka_kafkaNode_1 sh
> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafeidou --from-beginning
```