package br.com.juniorrodrigues;

import java.util.UUID;

public class CorrelationId {

    private final String id;

    public CorrelationId(String title) {
        this.id = title + "(" + UUID.randomUUID().toString() + ")";
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationId continueWith(String title) {//concatena o id atual com o novo gerado
        return new CorrelationId(id + "-" + title);
    }
}
