package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<String>> record);
    String getConsumerGroup();
    String getTopic();
}
