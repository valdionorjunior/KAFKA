package br.com.juniorrodrigues;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.beans.DesignMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {
    private final Connection connection;
    // Assumindo que esse serviço via dor uma unica vez, criarei uma conexão com o um banco de dados pra salvar aas informçeos
    public CreateUserService() throws SQLException {
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
    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class, Map.of())) {//incluso o tipo que espero de volta ao deserializar no map
            service.run();
            // try tenta executar o codigo se n conseguie, o kafka service fecha a conexão
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        var message = record.value();
        System.out.println("########################################");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());

        // pegando o valor que veio na mensagem
        var order = record.value().getPayload();
        if(isNewUser(order.getEmail())){
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser( String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)"); //o statement pra manipular o banco, no caso um insert
        insert.setString(1, UUID.randomUUID().toString());//gera novo uuid
        insert.setString(2, email);
        insert.execute();
        System.out.println("Usuario uuid e "+email+" adicionado.");
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users " +
                "where email = ? limit 1");// traz o usuario pelo uuid onde o email é igual ao que vir, onde sempre trara somente 1 registro
        exists.setString(1, email);
        var result = exists.executeQuery();

        return !result.next(); // se vai pra procima linha é ppq existe usuario

    }

}
