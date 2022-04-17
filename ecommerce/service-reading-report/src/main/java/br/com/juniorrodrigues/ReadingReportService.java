package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {
    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();


//CONSUMIDOR DO KAFKA
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var readingReportService = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GERERATE_READING_REPORT",
                readingReportService::parse,
                Map.of())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a conexão
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
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
