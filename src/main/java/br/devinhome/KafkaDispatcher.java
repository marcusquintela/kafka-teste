package br.devinhome;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher  implements Closeable {

    private final KafkaProducer<String, String> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<String, String>(getProperties());
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
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

    public void send(TopicsFromKafka topic, String key, String value) throws ExecutionException, InterruptedException {

        var record = new ProducerRecord<>(topic.name(), key, value);
        producer.send(record, getCallback()).get();
    }

    @Override
    public void close()  {
        producer.close();
    }
}
