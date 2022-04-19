package br.com.juniorrodrigues;

import br.com.juniorrodrigues.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try(orderDispatcher) { // KafkaDispatcher vai ser do tipo order, preciso de um tbm pra envio de email

            // Não estamo validando questao de segurança
            // Estamos apenas usando http de forma iniciante, para estudo.

            var email = req.getParameter("email");

            //criando pedidos
            var orderId = UUID.randomUUID().toString();// id do pedido
            var amount = new BigDecimal(req.getParameter("amount")); // numero vindo do parametro da chamada

            var order = new Order(orderId, amount, email);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);// passando topico a ser criado no kafka - email como chave

            var emailCode= "Thank you for your order! We are processing your order!";// email pra disparo - email como chave
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), emailCode);

            System.out.println("New Order sent sucessfuly!!!");

            // status Code de resposta
            resp.setStatus(HttpServletResponse.SC_OK);
            // enviando reposta pra chamada http
            resp.getWriter().println("New Order sent!");

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }

        // try tenta executar o codigo se n conseguie, o KafkaDispatcher  fecha a conexão

    }
}
