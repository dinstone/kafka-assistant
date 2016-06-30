
package com.dinstone.kafka.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.dinstone.kafka.client.ConsumerFactory;
import com.dinstone.kafka.client.consumer.ConsumerConfig;

public class ConsumerFactoryTest {

    public void testCreateConsumer() throws IOException {
        ConsumerConfig consumeConfig = new ConsumerConfig("config-consumer-test.xml");
        ConsumerFactory<String, String> cf = new ConsumerFactory<String, String>(consumeConfig.getKafkaConfig(), null,
            null);
        Consumer<String, String> consumer = cf.createConsumer();
        consumer.subscribe(Arrays.asList(consumeConfig.getTopic()));

        Set<TopicPartition> tps = consumer.assignment();
        consumer.seekToEnd(tps.toArray(new TopicPartition[0]));

        for (TopicPartition topicPartition : tps) {
            OffsetAndMetadata cos = consumer.committed(topicPartition);
            System.out.println(topicPartition + ":" + cos);
        }

        skip(consumer);

        // ConsumerRecords<String, String> records = consumer.poll(1000);
        // for (Iterator<ConsumerRecord<String, String>> iterator = records.iterator(); iterator.hasNext();) {
        // ConsumerRecord<String, String> record = iterator.next();
        // TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        //
        // OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
        // consumer.commitSync(Collections.singletonMap(tp, offset));
        // }

        // TopicPartition partition = new TopicPartition(consumeConfig.getTopic(), 0);
        // OffsetAndMetadata offset = new OffsetAndMetadata(9581694);
        // consumer.commitSync(Collections.singletonMap(partition, offset));

        // OffsetAndMetadata ofs = consumer.committed(partition);
        // System.out.println(partition + " : " + ofs);

    }

    protected void skip(Consumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            Set<TopicPartition> tps = records.partitions();
            for (TopicPartition topicPartition : tps) {
                List<ConsumerRecord<String, String>> prs = records.records(topicPartition);
                if (prs.size() > 0) {
                    ConsumerRecord<String, String> record = prs.get(prs.size() - 1);
                    long offset2 = record.offset() + 1;
                    OffsetAndMetadata offset = new OffsetAndMetadata(offset2);
                    consumer.commitSync(Collections.singletonMap(topicPartition, offset));

                    System.out.println(topicPartition + " : " + prs.size() + " : " + offset2);
                }
            }
        }
    }

    public void seekToEnd() throws IOException {
        ConsumerConfig consumeConfig = new ConsumerConfig("config-consumer-test.xml");
        ConsumerFactory<String, String> cf = new ConsumerFactory<String, String>(consumeConfig.getKafkaConfig(), null,
            null);
        Consumer<String, String> consumer = cf.createConsumer();

        List<PartitionInfo> piList = consumer.partitionsFor(consumeConfig.getTopic());
        List<TopicPartition> tps = new ArrayList<TopicPartition>(piList.size());
        for (PartitionInfo partitionInfo : piList) {
            tps.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(tps);

        for (TopicPartition topicPartition : tps) {
            long p = consumer.position(topicPartition);
            System.out.println(topicPartition + " : " + p);
        }
        consumer.seekToEnd(tps.toArray(new TopicPartition[0]));

        for (TopicPartition topicPartition : tps) {
            OffsetAndMetadata cos = consumer.committed(topicPartition);
            System.out.println(topicPartition + ":" + cos);
        }
    }

    public static void main(String[] args) throws IOException {
        ConsumerFactoryTest cft = new ConsumerFactoryTest();
        System.out.println("start");

        cft.testCreateConsumer();
        // cft.seekToEnd();

        System.in.read();

        System.out.println("stop");
    }

}
