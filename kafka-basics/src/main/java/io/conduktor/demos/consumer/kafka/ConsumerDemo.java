package io.conduktor.demos.consumer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("Starting Consumer Demo");

        String groupId = "my-java-application";
        String topic = "demo_java";

        KafkaConsumer<String, String> consumer = getKafkaConsumer(getProperties(groupId));
        subscribeAndPollMessages(consumer, topic);
    }

    public static void subscribeAndPollMessages(KafkaConsumer<String, String> consumer, String topic) {

        try {
            // subscribe to a topic
            consumer.subscribe(List.of(topic));

            // Poll for data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    log.info("Key : " + record.key() + ", Value : " + record.value());
                    log.info("Partition : " + record.partition() + ", Offset : " + record.offset());
                });
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }

    public static KafkaConsumer<String, String> getKafkaConsumer(Properties properties) {
        return new KafkaConsumer<>(properties);
    }

    public static Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
