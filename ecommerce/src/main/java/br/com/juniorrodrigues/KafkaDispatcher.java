package br.com.juniorrodrigues;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Classe kafka producer, como existe ja, criei o kafka dispatcher
 * */
class KafkaDispatcher implements Closeable {//necessario implementar Closeable pra fechar a conexão que foi aberta

    private final KafkaProducer<String, String> producer;

    KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }
    
    private static Properties properties() {
        var propertie = new Properties();
        propertie.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propertie.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertie.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propertie;
    }

    void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);//passando topico a ser criado no kafka
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/offset " + data.offset() + "/timestemp " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close(){
        producer.close();//fecha a conecção pro kafka
    }
}
