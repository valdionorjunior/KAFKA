package br.com.juniorrodrigues.consumer;

import br.com.juniorrodrigues.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {
    // you may argue that a ConsumerException would be better
    // and its ok, it can be better
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
    String getConsumerGroup();
    String getTopic();
}
