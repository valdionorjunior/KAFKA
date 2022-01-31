package br.com.juniorrodrigues;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {

            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();//chave define qual partição vai a mensagem, =aqui usamos o id(id simulado) do user pra isso
                var value = key + "1237897898213,5435,77";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);//passando topico a ser criado no kafka

                var email = "Thank you for your order! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
            // try tenta executar o codigo se n conseguie, o KafkaDispatcher  fecha a conexão
        }

    }

}
