package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;
    // Assumindo que esse serviço via dor uma unica vez, criarei uma conexão com o um banco de dados pra salvar aas informçeos
    public BatchSendMessageService() throws SQLException {
        // aqui no construtor vamos aabrir a conexao com o banco
        String url ="jdbc:sqlite:target/users_database.db";// criando o arquivo dentro do diretotio target do projeto
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        }catch (SQLException ex){
            // be careful, the sql could be wrong, be really careful
            ex.printStackTrace();
        }
    }

    // CONSUMIDOR DO KAFKA
    public static void main(String[] args) throws ExecutionException, InterruptedException, SQLException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a conexão
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {
        var message = record.value();
        System.out.println("########################################");
        System.out.println("Processing new batch");
        System.out.println("Topic: " + message.getPayload());

        // mandando mensagem pra todos os usuario
        for(User user: getAllUsers()){
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(), message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),user);
            System.out.println("Acho que enviei para " + user);
        }
    }

    private List <User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();

        List <User> users = new ArrayList<>();
        while(results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }

}
