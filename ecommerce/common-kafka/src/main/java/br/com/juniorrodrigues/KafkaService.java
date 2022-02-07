package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
//refeactor pra classe trabalhar com generics pra desserializar
class KafkaService<T> implements Closeable {//necessario implementar Closeable pra fechar a coneção que foi aberta
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {//incluido tipe de volta / adicionamos um map de propriedade extras para serem passada para serialização
         this(groupId, parse, type,properties);
        consumer.subscribe(Collections.singletonList(topic)); //consumindo a msg de algum topic,
//        this.run();
    }
    //segundo construtor
    KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {//incluido tipe de volta
        this(groupId, parse, type, properties);
        consumer.subscribe(topic); //consumindo a msg de algum topic sob o regex do que esta vindo em string,
//        this.run();
    }

    private KafkaService(String groupId, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
         this.parse= parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));// verificando se tem msg no topico por algum tempo (100 milisec)
            if (!records.isEmpty()) {
                System.out.println("Encontrado"+records.count()+" registros");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (ExecutionException e) {
                        // aqui por enquanto apenas logar as mensagens de execption para n para o serviço
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // aqui por enquanto apenas logar as mensagens de execption para n para o serviço
                        e.printStackTrace();
                    }
                    System.out.println("########################################");
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overridProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //de byte pra string
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());//desserializar com GSON -> classe que criamos
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);//id para grupo diferente para o consumo de msg pra email
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());//client Id
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());//criamos uma propriedade na classe GsonDeserializer, para passar o tipo que é o dado pra depois descerializarmos, por padrao via se uma string
        properties.putAll(overridProperties);//adicionando as propriedade adicionaisque estou repassando.
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
