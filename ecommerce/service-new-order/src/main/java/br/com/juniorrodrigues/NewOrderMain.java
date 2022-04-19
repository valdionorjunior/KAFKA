package br.com.juniorrodrigues;

import br.com.juniorrodrigues.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) { //KafkaDispatcher vai ser do tipo order, preciso de um tbm pra envio de email
            var email = String.valueOf(Math.random()).concat("@email.com");//email ficticio
            for (var i = 0; i < 10; i++) {
                //criando pedidos
                var orderId = UUID.randomUUID().toString();//id do pedido
                var amount = new BigDecimal(Math.random() * 5000 + 1); //numero entre 0 e 5000 mas que seja pelo menos onumero 1
                var id = new CorrelationId(NewOrderMain.class.getSimpleName());
                var order = new Order(orderId, amount, email);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);//passando topico a ser criado no kafka - email como chave

            }
        }

    }

}
