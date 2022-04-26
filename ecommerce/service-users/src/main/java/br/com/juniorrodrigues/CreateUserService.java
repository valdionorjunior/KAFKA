package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    // CONSUMIDOR DO KAFKA
    public static void main(String[] args) {
        // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner<>(CreateUserService::new).start(1);// pasando o numero de threads que quero que ele rode, aqu item q ser 1 por thread pq é conection com banco
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
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
        var uuid = UUID.randomUUID().toString();
        database.update("insert into Users (uuid, email) values (?,?)", uuid, email);
        System.out.println("Usuario "+ uuid +" e "+email+" adicionado.");
    }

    private boolean isNewUser(String email) throws SQLException {
        var result = database.query("select uuid from Users " +
                "where email = ? limit 1", email);
        return !result.next(); // se vai pra procima linha é ppq existe usuario

    }

}
