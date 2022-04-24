package br.com.juniorrodrigues;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
