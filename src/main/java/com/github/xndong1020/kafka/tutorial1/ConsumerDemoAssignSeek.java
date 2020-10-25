package com.github.xndong1020.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String boostrapServers = "http://localhost:9092";
        String topic = "first_topic";

        // create Producer properties
        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        // set up deserializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        // "earliest" means read from very beginning of the topic, "latest" meaning new messages onwards,
        // "none" will thrown an error if no offset has been saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek does NOT use groupId, it mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 5L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        // tells consumer to read from which topic partition, and seek from a given offset
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        final int numberOfMessageToRead = 5;
        boolean isContinueReading = true;
        int counter = 0;

        // poll from new data
        while(isContinueReading) {
                //consumer.poll(1000);
           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

           for (ConsumerRecord<String, String> record: records) {
               counter++;
               logger.info("Key:" + record.key() + "\n" +
                       "Value:" + record.value() + "\n" +
                       "Partition:" + record.partition() + "\n" +
                       "Offset:" + record.offset());
               if (counter >= numberOfMessageToRead) {
                   isContinueReading = false;
                   break;
               }
           }
        }
    }
}
