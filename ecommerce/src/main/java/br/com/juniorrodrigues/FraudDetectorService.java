package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::parse, Order.class)) {//incluso o tipo que espero de volta ao deserializar
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a conexão
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("########################################");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000); //espera 5 sec so pr a simular um temp ode processamento aqui.
        } catch (InterruptedException e) {
            //ignore, tratamento somente pq é obrigatori pro nosso teste
            e.printStackTrace();
        }
        System.out.println("Order processed!");
        System.out.println("########################################");
    }


}
