package br.com.juniorrodrigues;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {
    private final LocalDatabase database;

    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
         // you might want to safe all data
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key )");
    }

    public boolean saveNew(Order order) throws SQLException {
        if(wasProcessing(order)){
            return false;
        }
        database.update("insert into Orders (uuid) values (?)", order.getOrderId());// inserir o registro de order
        return true;
    }

    private boolean wasProcessing(Order order) throws SQLException {
        var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());// verificando se foi processada o pedido
        return result.next();//se tem proximo, a order ta la, ja foi processada.
    }

    @Override
    public void close() throws IOException {
        try {
            database.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
