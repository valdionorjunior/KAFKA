package br.com.juniorrodrigues;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

// classe para deserializar as mensagens
public class GsonDeserializer<T> implements Deserializer<T> {
    //implementa a classe Deserializer para realmente fazer o trabalhjo de voltar o dado ao normal

    public static final String TYPE_CONFIG = "br.com.juniorrodrigues.type_config";//propriedade que indica o tipo do item a ser deserializado
    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;//variavel mebro do tipo generics

    //sobreescrevemos o metodo configure para que possamos passar o tipo que o dado vai ser deserializado.
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(TYPE_CONFIG));//puxa a configuraçlão do tiupo do dado
        try {//jogos num try pq pode não existir a classe
            this.type = (Class<T>) Class.forName(typeName);//tranforma numa classe, forçando o cast pra generics
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Type for deserialization does not exit in the classpath.", e); // faço da erro pq deu erro ao criar a classe com o tipo passado.
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {//recebe os bite e deserializa passando o tipo que o dado era antes (pode ser um dado String, ou um objeto e vc não sabe isso -> vide metodo configure)
        return gson.fromJson(new String(bytes), type);
    }
}
