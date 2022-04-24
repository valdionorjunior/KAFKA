package br.com.juniorrodrigues.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {
    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }

    public void start(int threadCount){
        var pool= Executors.newFixedThreadPool(threadCount); // executa o numero de threads que eu quizer
        for(int i=0; i<= threadCount; i++) {
            pool.submit(provider);
        }
    }
}
