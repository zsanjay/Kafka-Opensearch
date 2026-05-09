package io.conduktor.demos.producer.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        LOG.info("Starting ProducerDemoWithCallback");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {

                String topic = "demo_java";
                String key = "id_" + j;
                String value = "hello world " + j;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(record, (recordMetadata, e) -> {
                        if (e != null) {
                            LOG.error("Error while producing", e);
                        } else {
                            LOG.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                        }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all the data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
