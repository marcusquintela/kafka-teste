package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Collections.singleton(TopicsFromKafka.ECOMMERCE_SEND_EMAIL.name()));
//        consumer.poll(0);
//        consumer.seekToBeginning(consumer.assignment());
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("-----------------------------------------");
                System.out.println("Find "+records.count()+" email.");
                records.forEach(stringStringConsumerRecord -> {
                    System.out.println("Send email "
                            + stringStringConsumerRecord.topic() + ":::"
                            + stringStringConsumerRecord.key() + "/"
                            + stringStringConsumerRecord.partition() + "/"
                            + stringStringConsumerRecord.offset() + "/"
                            + stringStringConsumerRecord.timestamp());
                    System.out.println("Order processed.");
                });
            }
        }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }
}
