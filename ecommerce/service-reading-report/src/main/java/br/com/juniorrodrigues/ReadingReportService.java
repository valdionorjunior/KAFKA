package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class ReadingReportService implements ConsumerService<User> {
    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

//CONSUMIDOR DO KAFKA
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner(ReadingReportService::new).start(5);// pasando o numero de threads que quero que ele rode
    }

    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_USER_GERERATE_READING_REPORT";
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("########################################");
        System.out.println("Processing report for " + record.value());

        //geração do arquivo de relatorio - atravez do modelo report.txt
        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());

        IO.copyTo(SOURCE, target);
        IO.append(target,"CREATED for "+ user.getUuid()); //adiciona no arquivo o texto passado;

        System.out.println("File created: " + target.getAbsolutePath());
        System.out.println("########################################");
    }
}
