package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.avro.Message;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var config = new Properties() {{
            put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
            put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            put("client.id", "test");
            put("group.id", "foo");
            put("bootstrap.servers", "localhost:9092");
        }};

        var encoder = Message.getEncoder();

        try (var producer = new KafkaProducer<Integer, byte[]>(config)) {
            producer.send(
                new ProducerRecord<>(
                    "quickstart-events", 1, encoder.encode(new Message("xxx", "yyy")).array()
                )
            ).get();
        }

        System.out.println("Hello World!");
    }
}