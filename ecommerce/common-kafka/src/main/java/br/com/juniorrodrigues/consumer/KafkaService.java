package br.com.juniorrodrigues.consumer;

import br.com.juniorrodrigues.Message;
import br.com.juniorrodrigues.dispatcher.GsonSerializer;
import br.com.juniorrodrigues.dispatcher.KafkaDispatcher;
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
public class KafkaService<T> implements Closeable {//necessario implementar Closeable pra fechar a coneção que foi aberta
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {//incluido tipe de volta / adicionamos um map de propriedade extras para serem passada para serialização
         this(groupId, parse,properties);
        consumer.subscribe(Collections.singletonList(topic)); //consumindo a msg de algum topic,
//        this.run();
    }
    //segundo construtor
     public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {//incluido tipe de volta
        this(groupId, parse, properties);
        consumer.subscribe(topic); //consumindo a msg de algum topic sob o regex do que esta vindo em string,
//        this.run();
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> properties) {
         this.parse= parse;
        this.consumer = new KafkaConsumer<>(getProperties( groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        //criando um novo dispatcher
        try(var deadLetter = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));// verificando se tem msg no topico por algum tempo (100 milisec)
                if (!records.isEmpty()) {
                    System.out.println("Registros encontrados: " + records.count());
                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            // Não importa o que aconteça eu quero pegar a proxima mensagem, casos raros em que se usa Excption no consumer
                            // aqui por enquanto apenas logar as mensagens de execption para n para o serviço
                            e.printStackTrace();
                            // Tratando erros de consumo de mesngem
                            // envia uma mensagem pro topico dizendo que deu erro ao consumir
                            var message = record.value();
                            //cria um novo dispatcher
                            deadLetter.send("ECOMMERCE_DEADLETTER",
                                    message.getId().toString(), //correlationId da mensagem que deu erro
                                    message.getId().continueWith("DeadLetter"), // cria um id novo pra menseage de emvio que deu erro para saber qua lé a mensagem com erro
                                    new GsonSerializer().serialize("", message) // mensagen pode ser de qualquer tipo por isso passo um Gson serializer generico resializando a mensagem
                            );
                        }
                        System.out.println("########################################");
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overridProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //de byte pra string
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());//desserializar com GSON -> classe que criamos
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);//id para grupo diferente para o consumo de msg pra email
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());//client Id
//        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());//criamos uma propriedade na classe GsonDeserializer, para passar o tipo que é o dado pra depois descerializarmos, por padrao via se uma string
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");//definindo o numero maximo de consumo de mensagens por vez, nesse caso aqui 1
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // ajusta quando n tem offset inicial, ou n existe mais offset no servidor, por padrao ele começa do ultimo, largest significa o qeu es estavamos fazendo
        //AUTO_OFFSET_RESET_CONFIG - cuidado, essa config pode ser diferente para versoes diferentes do kafka
        properties.putAll(overridProperties);//adicionando as propriedade adicionaisque estou repassando.
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
