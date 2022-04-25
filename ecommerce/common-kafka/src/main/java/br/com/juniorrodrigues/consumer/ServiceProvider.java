package br.com.juniorrodrigues.consumer;

import java.util.HashMap;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> { // fazendo rodar varia vezes  - Callable é do tipo V, to fazendo ele devolver um <Void>
    private final ServiceFactory<T> factory;
    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception {
        var myService = factory.create(); // quando ela cria, devolve o email service
        try (var service = new KafkaService(myService.getConsumerGroup(),
                myService.getTopic(),
                myService::parse,
                new HashMap<>())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a nenexão
        }
        return null; // pq devolve um Void Object
    }
}
