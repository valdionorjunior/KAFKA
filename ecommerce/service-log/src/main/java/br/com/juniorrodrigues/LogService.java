package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService implements ConsumerService<String> {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args){
        // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner(LogService::new).start(5);// pasando o numero de threads que quero que ele rode

    }

    public String getConsumerGroup() {
        return LogService.class.getSimpleName();
    }

    public String getTopic() {
        return String.valueOf(Pattern.compile("ECOMMERCE.*"));
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {

        System.out.println("########################################");
        System.out.println("LOG: " + record.topic());//nome do topico de onde veio a msg
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

    }

}
