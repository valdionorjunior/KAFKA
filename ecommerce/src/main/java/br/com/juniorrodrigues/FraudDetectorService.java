package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FraudDetectorService {
//CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER")); //consumindo a msg de algum topic,
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));// verificando se tem msg no topico por algum tempo (100 milisec)

            if (!records.isEmpty()) {
                System.out.println("Encontrado"+records.count()+" registros");
                for (var record : records) {
                    System.out.println("########################################");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());

                    try {
                        Thread.sleep(5000); //espera 5 sec so pr a simular um temp ode processamento aqui.
                    } catch (InterruptedException e) {
                        //ignore, tratamento somente pq é obrigatori pro nosso teste
                        e.printStackTrace();
                    }
                    System.out.println("Order processed!");
                    System.out.println("########################################");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //de byte pra string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());//preciso definir um nome de um grupo de consumo, pq eu posso ter mais de uam api consumindo as mensagens, nesse caso to passando o nome da nossa classe
        return properties;
    }
}