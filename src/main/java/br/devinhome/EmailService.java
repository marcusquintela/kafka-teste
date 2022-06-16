package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafKaService(EmailService.class.getSimpleName(), TopicsFromKafka.ECOMMERCE_SEND_EMAIL, emailService::parse);
        service.run();
    }
    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Send email "
                + record.topic() + ":::"
                + record.key() + "/"
                + record.partition() + "/"
                + record.offset() + "/"
                + record.timestamp());
    }



}
