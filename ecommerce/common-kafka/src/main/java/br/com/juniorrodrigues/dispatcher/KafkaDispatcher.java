package br.com.juniorrodrigues.dispatcher;

import br.com.juniorrodrigues.CorrelationId;
import br.com.juniorrodrigues.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Classe kafka producer, como existe ja, criei o kafka dispatcher
 * */
public class KafkaDispatcher<T> implements Closeable {//necessario implementar Closeable pra fechar a conexão que foi aberta // implementamos a classe omc generics para poder receber qualquer valor e enviar ao kafka

    private final KafkaProducer<String, Message<T>> producer;// producer tbm tem que receber um valor de string e valor generico
    public KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }



    private static Properties properties() {
        var propertie = new Properties();
        propertie.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propertie.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        propertie.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // tranforma String em bites (serialização)
        propertie.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName()); // tranforma objetos em json (serialização)
        propertie.setProperty(ProducerConfig.ACKS_CONFIG, "all"); /*configuracao para que quando o leader sempre replique tudo para as replicas, é lento, mas da a agarantia que quando ele cair a replica se assumuir esteja equalizada
        ou seja, vai dar  o ok de que toda a request foi completada, repassando tbm para as replicas, precisa ver as configurações de porametros de ACKS */

        return propertie;
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {//envio asincrono
        var value = new Message<>(id.continueWith("_"+topic), payload);//implementando correlationId + menasge que antes vinha do tipo generico T de forma envelopada
        var record = new ProducerRecord<>(topic, key, value);//passando topico a ser criado no kafka
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/offset " + data.offset() + "/timestemp " + data.timestamp());
        };
        return  producer.send(record, callback);
    }

    @Override
    public void close(){
        producer.close();//fecha a conecção pro kafka
    }
}
