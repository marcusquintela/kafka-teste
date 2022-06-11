package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(getProperties());
        // Expressão regular  para comsumir todos os topico iniciados com ECOMMERCE
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
//        consumer.poll(0);
//        consumer.seekToBeginning(consumer.assignment());
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("-----------------------------------------");
                System.out.println("Find "+records.count()+" records.");
                records.forEach(stringStringConsumerRecord -> {
                    System.out.println("Log "
                            + stringStringConsumerRecord.topic() + ":::"
                            + stringStringConsumerRecord.key() + "/"
                            + stringStringConsumerRecord.partition() + "/"
                            + stringStringConsumerRecord.offset() + "/"
                            + stringStringConsumerRecord.timestamp());
                });
            }
        }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
