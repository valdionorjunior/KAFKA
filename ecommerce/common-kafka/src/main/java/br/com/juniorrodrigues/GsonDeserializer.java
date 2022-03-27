package br.com.juniorrodrigues;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

// classe para deserializar as mensagens
public class GsonDeserializer implements Deserializer<Message> {
    //implementa a classe Deserializer para realmente fazer o trabalhjo de voltar o dado ao normal

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create(); // serializador, devolve o json, ele direto não funciona pro kafka
    // apos ter mudado a serialização precisei passar o tupo que vai ser serializado registerTypeAdapter ->  MessageAdapter


    @Override
    public Message deserialize(String s, byte[] bytes) {//recebe os bite e deserializa passando o tipo que o dado era antes (pode ser um dado String, ou um objeto e vc não sabe isso -> vide metodo configure)
        return gson.fromJson(new String(bytes), Message.class);
    }
}
