package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import br.com.juniorrodrigues.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner(EmailNewOrderService::new).start(5);// pasando o numero de threads que quero que ele rode
    }
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    //enviar mensagme tbm agora
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("########################################");
        System.out.println("Processing new order, prepering email.");
        var message = record.value();
        System.out.println(message);

        var order = message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        var emailCode= "Thank you for your order! We are processing your order!";//email pra disparo - email como chave
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                id, emailCode);
    }
}
