package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

class KafKaService {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFuction parse;

     KafKaService(String groupId, TopicsFromKafka ecommerceSendEmail, ConsumerFuction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<String, String>(getProperties(groupId));
        consumer.subscribe(Collections.singleton(TopicsFromKafka.ECOMMERCE_SEND_EMAIL.name()));
//        consumer.poll(0);
//        consumer.seekToBeginning(consumer.assignment());

    }

    private static Properties getProperties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId+"-"+UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
        return properties;
    }

     void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("-----------------------------------------");
                System.out.println("Find " + records.count() + " email.");
                records.forEach(stringStringConsumerRecord -> {
                    parse.cosume(stringStringConsumerRecord);
                });
            }
        }
    }
}
