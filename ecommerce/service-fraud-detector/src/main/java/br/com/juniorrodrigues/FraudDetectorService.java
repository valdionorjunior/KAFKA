package br.com.juniorrodrigues;

import br.com.juniorrodrigues.consumer.ConsumerService;
import br.com.juniorrodrigues.consumer.ServiceRunner;
import br.com.juniorrodrigues.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {
    private final LocalDatabase database;
//CONSUMIDOR DO KAFKA

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean )");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
    // rodando varios emails services, atravez do call do provider, falando qual a function que cria um email service
        new ServiceRunner(FraudDetectorService::new).start(1);// pasando o numero de threads que quero que ele rode
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
    //enviar mensagme tbm agora
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("########################################");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        var message = record.value();
        //pegando o valor que veio na mensagem
        var order = message.getPayload();

        if(wasProcessing(order)){
            System.out.println("Order"+ order.getOrderId() +" was a already processed! S(Order já processada!)");
            return;
        }

        try {
            Thread.sleep(5000); //espera 5 sec so pr a simular um temp ode processamento aqui.
        } catch (InterruptedException e) {
            //ignore, tratamento somente pq é obrigatori pro nosso teste
            e.printStackTrace();
        }

        if(isaFraud(order)){
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());// inserir o registro de fraude pra aquela order
            //simulando que quando o valor do pedido for >= 4500 vai ser um pedido de rfaude
            System.out.println("Order is a fraud! " + order);
            //enviando mensagem de order com fraude, criando um novo topico caso n exista
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }else{
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());// inserir o registro de fraude pra aquela order
            System.out.println("Aproved: "+ order);
            //enviando mensagem de order aprovada sem fraude, criando um novo topico caso n exista
            orderDispatcher.send("ECOMMERCE_ORDER_APROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
    }

    private boolean wasProcessing(Order order) throws SQLException {
        var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());// verificando se foi processada o pedido
        return result.next();//se tem proximo, a order ta la, ja foi processada.
    }

    private boolean isaFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
