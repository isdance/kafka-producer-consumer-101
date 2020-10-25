package com.github.xndong1020.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        String boostrapServers = "http://localhost:9092";
        String groupId = "my-fifth-app";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(boostrapServers, groupId, topic, latch);

        // run myConsumerRunnable on a new thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).Shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application is closing");
            }

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;


        public ConsumerRunnable(String boostrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

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
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            // consumer.subscribe(Collections.singleton("first_topic"));
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // poll from new data
            try {
                while (true) {
                    //consumer.poll(1000);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key:" + record.key() + "\n" +
                                "Value:" + record.value() + "\n" +
                                "Partition:" + record.partition() + "\n" +
                                "Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // let our main application know that we've done the consumer, and should exist
                latch.countDown();
            }
        }

        public void Shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
