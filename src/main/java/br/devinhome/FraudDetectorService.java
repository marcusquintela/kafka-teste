package br.devinhome;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudeService = new FraudDetectorService();
        var service = new KafKaService(FraudDetectorService.class.getSimpleName(), TopicsFromKafka.ECOMMERCE_ORDER_NEW, fraudeService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Processing new order, checking for froud "
                + record.topic() + ":::"
                + record.key() + "/"
                + record.partition() + "/"
                + record.offset() + "/"
                + record.timestamp());
        System.out.println("Order processed.");
    }

}
