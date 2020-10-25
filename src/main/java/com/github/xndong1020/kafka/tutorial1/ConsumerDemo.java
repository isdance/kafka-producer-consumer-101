package com.github.xndong1020.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String boostrapServers = "http://localhost:9092";
        String groupId = "my-third-app";

        // create Producer properties
        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        // set up deserializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // set up group id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // "earliest" means read from very beginning of the topic, "latest" meaning new messages onwards,
        // "none" will thrown an error if no offset has been saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        // consumer.subscribe(Collections.singleton("first_topic"));
        consumer.subscribe(Arrays.asList("first_topic"));

        // poll from new data
        while(true) {
                //consumer.poll(1000);
           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

           for (ConsumerRecord<String, String> record: records) {
               logger.info("Key:" + record.key() + "\n" +
                       "Value:" + record.value() + "\n" +
                       "Partition:" + record.partition() + "\n" +
                       "Offset:" + record.offset());
           }
        }
    }
}
