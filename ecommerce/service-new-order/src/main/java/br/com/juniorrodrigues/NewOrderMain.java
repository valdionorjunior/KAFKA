package br.com.juniorrodrigues;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) { //KafkaDispatcher vai ser do tipo order, preciso de um tbm pra envio de email
            try (var emailDispatcher = new KafkaDispatcher<String>()) { // preciso de um tbm pra envio de email do tipo string

                for (var i = 0; i < 10; i++) {
                    //criando pedidos
                    var userId = UUID.randomUUID().toString();//chave define qual partição vai a mensagem, =aqui usamos o id(id simulado) do user pra isso
                    var orderId = UUID.randomUUID().toString();//id do pedido
                    var amount = new BigDecimal(Math.random() * 5000 + 1); //numero entre 0 e 5000 mas que seja pelo menos onumero 1
                    var order = new Order(userId, orderId, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);//passando topico a ser criado no kafka

                    var email = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
                // try tenta executar o codigo se n conseguie, o KafkaDispatcher  fecha a conexão
            }
        }

    }

}
