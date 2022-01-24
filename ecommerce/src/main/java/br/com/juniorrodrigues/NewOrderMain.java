package br.com.juniorrodrigues;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var prducer = new KafkaProducer<String, String>(properties());
        var value = "1237897898213,5435,77";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);//passando topico a ser criado no kafka
        prducer.send(record, (data, ex) -> {
            if(ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando "+data.topic()+ ":::partition "+ data.partition()+ "/offset "+ data.offset() + "/timestemp "+ data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var propertie = new Properties();
        propertie.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propertie.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertie.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return propertie;
    }
}
