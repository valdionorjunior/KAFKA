package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.KafkaService;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ServiceProvider {
    public <T> void run(ServiceFactory<T> factory) throws ExecutionException, InterruptedException {// metodo que recebe um generico
        var emailService = factory.create(); // quando ela cria, devolve o email service
        try (var service = new KafkaService(emailService.getConsumerGroup(),
                emailService.getTopic(),
                emailService::parse,
                new HashMap<>())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a nenexão
        }
    }
}
