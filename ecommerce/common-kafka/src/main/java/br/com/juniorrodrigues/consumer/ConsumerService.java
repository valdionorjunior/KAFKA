package br.com.juniorrodrigues.consumer;

import br.com.juniorrodrigues.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<T>> record) throws IOException, ExecutionException, InterruptedException;
    String getConsumerGroup();
    String getTopic();
}
