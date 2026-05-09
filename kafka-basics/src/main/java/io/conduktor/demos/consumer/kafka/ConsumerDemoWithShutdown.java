package io.conduktor.demos.consumer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("Starting Consumer Demo with Shutdown");

        String groupId = "my-java-application";
        String topic = "demo_java";

        KafkaConsumer<String, String> consumer = ConsumerDemo.getKafkaConsumer(ConsumerDemo.getProperties(groupId));

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
