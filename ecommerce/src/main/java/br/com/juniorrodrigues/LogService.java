package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService(LogService.class.getSimpleName(),Pattern.compile("ECOMMERCE.*"),logService::parse, String.class)) {
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a nenexão
        }
    }

    private void parse(ConsumerRecord<String, String> record) {

        System.out.println("########################################");
        System.out.println("LOG: " + record.topic());//nome do topico de onde veio a msg
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

    }

}
