package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class EmailService implements ConsumerService<String> {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // rodando varios emails services, atraz do run do provider, falando qual a function que cria um email service
        new ServiceProvider().run(EmailService::new);
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("########################################");
        System.out.println("Send email.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000); //espera 1 sec so pr a simular um temp ode processamento aqui.
        } catch (InterruptedException e) {
            //ignore, tratamento somente pq é obrigatori pro nosso teste
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }


}
