package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;
//refector para a interface aceito generics
public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, T> record);
}
