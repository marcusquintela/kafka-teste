package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFuction {
    void cosume(ConsumerRecord<String,String> record);
}
