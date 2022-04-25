package br.com.juniorrodrigues.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create() throws Exception;
}
