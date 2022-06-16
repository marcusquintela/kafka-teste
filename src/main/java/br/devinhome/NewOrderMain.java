package br.devinhome;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 100; i++) {

                var key = UUID.randomUUID().toString();
                var value = "132123,67523,7894589745";
                dispatcher.send(TopicsFromKafka.ECOMMERCE_ORDER_NEW, key, value);


                var email = "Hello, Thank you for uoar order! We are processing your order! ";
                dispatcher.send(TopicsFromKafka.ECOMMERCE_SEND_EMAIL, key, email);
            }
        }
    }


}
