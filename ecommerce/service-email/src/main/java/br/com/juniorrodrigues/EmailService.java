package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args){
        // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner(EmailService::new).start(5);// pasando o numero de threads que quero que ele rode

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
