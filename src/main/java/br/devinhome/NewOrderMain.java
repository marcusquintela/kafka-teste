package br.devinhome;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(getProperties());
        var value = "132123,67523,7894589745";

        var record = new ProducerRecord<>(TopicsFromKafka.ECOMMERCE_ORDER_NEW.name(), value, value);
        producer.send(record, getCallback()).get();

        var email = "Hello, Thank you for uoar order! We are processing your order! ";
        var emailRecord =  new ProducerRecord<>(TopicsFromKafka.ECOMMERCE_SEND_EMAIL.name(), email, email);
        producer.send(emailRecord, getCallback()).get();

    }

    private static Callback getCallback() {
        Callback callback = ((recordMetadata, e) -> {
            if (e != null) {
                e.printStackTrace();
                return;
            }
            System.out.println("Send sucess "
                    + recordMetadata.topic() + ":::"
                    + recordMetadata.partition() + "/"
                    + recordMetadata.offset() + "/"
                    + recordMetadata.timestamp());

        });
        return callback;
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
