package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL",emailService::parse, String.class)) {
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a nenexão
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
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
