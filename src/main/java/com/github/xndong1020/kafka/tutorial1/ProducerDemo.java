package com.github.xndong1020.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String boostrapServers = "http://localhost:9092";

        // create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", boostrapServers);
//        // serializer key and value into bytes for Kafka
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a key message
//        ProducerRecord<String, String> record =
//                new ProducerRecord<String, String>("first_topic", "second message");
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "my_key", "third message");

        // send data - asynchronous running in background
        // when program hit this line, the code is executed, running in the background, then exist
        // the message is never sent
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully sent, or an exception is thrown
                if (e != null) {
                    logger.error("Error while producing", e);
                } else {
                    logger.info("Received new metadata. \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
                }
            }
        });

        // flush data means force all data to be produced
        producer.flush();
        // flush and close connection
        // producer.close();
    }
}
