package org.example;

/**
 * Hello world!
 */

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.example.avro.Message;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("main thread started");

        var config = new Properties() {{
            put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
            put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            put("client.id", "test_consumer");
            put("group.id", "test_consumer_foo");
            put("bootstrap.servers", "localhost:9092");
            put("enable.auto.commit", "false");
            // TODO: disable auto commits
        }};

        var consumer = new KafkaConsumer<Integer, byte[]>(config);

        var consumerThread = new Thread(() -> {
            System.out.println("consumer thread is started");

            consumer.subscribe(List.of("quickstart-events"));

            try {
                while (true) {
                    for (var record : consumer.poll(Duration.ofMillis(500))) {
                        System.out.println(record.offset());
                        System.out.println(record.key());

                        var message = Message.getDecoder().decode(record.value());
                        System.out.println(message.getTitle());
                        System.out.println(message.getMessage());

                    }

                    System.out.println("batch processed");
                    consumer.commitSync();
                }
            } catch (WakeupException e) {
                System.out.println("wake up!!!");
                consumer.commitSync();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("close the consumer");
                consumer.close();
            }

            System.out.println("consumer thread is executed");
        });

        consumerThread.setName("my-consumer-thread");
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("stop");
            try {
                consumer.wakeup();
                consumerThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }
}
