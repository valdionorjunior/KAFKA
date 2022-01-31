package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService implements Closeable {//necessario implementar Closeable pra fechar a coneção que foi aberta
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse) {
         this(groupId, parse);
        consumer.subscribe(Collections.singletonList(topic)); //consumindo a msg de algum topic,
//        this.run();
    }
    //segundo construtor
    KafkaService(String groupId, Pattern topic, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(topic); //consumindo a msg de algum topic sob o regex do que esta vindo em string,
//        this.run();
    }

    private KafkaService(String groupId, ConsumerFunction parse) {
         this.parse= parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));// verificando se tem msg no topico por algum tempo (100 milisec)
            if (!records.isEmpty()) {
                System.out.println("Encontrado"+records.count()+" registros");
                for (var record : records) {
                    parse.consume(record);
                    System.out.println("########################################");
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //de byte pra string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);//id para grupo diferente para o consumo de msg pra email
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());//client Id
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
