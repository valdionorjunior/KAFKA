package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class, Map.of())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a conexão
        }
    }
    //enviar mensagme tbm agora
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
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
        //pegando o valor que veio na mensagem
        var order = record.value();

        if(isaFraud(order)){
            //simulando que quando o valor do pedido for >= 4500 vai ser um pedido de rfaude
            System.out.println("Order is a fraud! " + order);
            //enviando mensagem de order com fraude, criando um novo topico caso n exista
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        }else{
        System.out.println("Aproved: "+ order);
            //enviando mensagem de order aprovada sem fraude, criando um novo topico caso n exista
            orderDispatcher.send("ECOMMERCE_ORDER_APROVED", order.getUserId(), order);
        }
    }

    private boolean isaFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
