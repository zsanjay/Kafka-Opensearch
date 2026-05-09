package io.conduktor.demos.consumer.kafka;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {

        log.info("Starting Consumer Demo Cooperative");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = ConsumerDemo.getProperties(groupId);
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.put("group.instance.id", ""); // strategy for static assignments
        KafkaConsumer<String, String> consumer = ConsumerDemo.getKafkaConsumer(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                log.info("Main thread : {} starts joining", mainThread.getName());
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        ConsumerDemo.subscribeAndPollMessages(consumer, topic);
    }
}
