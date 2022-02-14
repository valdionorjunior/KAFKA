package br.com.juniorrodrigues;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpEcommerceService {
    public static void main(String[] args) throws Exception { // Rodando servidor com jetty

        var server = new Server(8080);

        // criando contexto que lida com as requisiçoes
        var context = new ServletContextHandler();
        context.setContextPath("/"); // caminho
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");

        server.setHandler(context);

        server.start();
        server.join();

    }
}
