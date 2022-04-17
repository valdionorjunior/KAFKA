package br.com.juniorrodrigues;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try{

            batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", "ECOMMERCE_USER_GERERATE_READING_REPORT",
                    new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()),"ECOMMERCE_USER_GERERATE_READING_REPORT"); //envia a mensagem para gerar o report para todo os usuarios;

            System.out.println("Sent generete report to all users!");
            // status Code de resposta
            resp.setStatus(HttpServletResponse.SC_OK);
            // enviando reposta pra chamada http
            resp.getWriter().println("Reports requests genereted!");

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }

        // try tenta executar o codigo se n conseguie, o KafkaDispatcher  fecha a conexão

    }
}
