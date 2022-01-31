package br.com.juniorrodrigues;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().create(); // serializador, devolve o json, ele direto não funciona pro kafka
    //implementamos entao na classe o serializer do kafka passando geric pra ele serializar qualquer coisa, não so no serializer mas na minha classe GSON tbm

    @Override
    public byte[] serialize(String s, T object) { //serializando o objeto para json e retorno em  bites
        return gson.toJson(object).getBytes();
    }


}
