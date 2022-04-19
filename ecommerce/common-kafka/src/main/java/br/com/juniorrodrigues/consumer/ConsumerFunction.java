package br.com.juniorrodrigues.consumer;

import br.com.juniorrodrigues.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

//refector para a interface aceito generics
public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
