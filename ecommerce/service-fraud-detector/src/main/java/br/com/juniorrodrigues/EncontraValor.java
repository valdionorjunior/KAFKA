package br.com.juniorrodrigues;

import java.util.Arrays;

public class EncontraValor {
    public static void main(String[] args) {
        int[] list ={1,2,3,4,5,6,7,9};
        var numeroFaltante = encontraValor(list);
        System.out.println("Fata o numero: "+ numeroFaltante);
    }

    public static int encontraValor(int[] lista){
        //Encontrar numero faltante de uma lista sequencial de 1 a N
        var n = lista.length +1;
        var soma = n*(n+1)/2;
        var somaValores = Arrays.stream(lista).reduce(0, Integer :: sum);

        return (int)soma - somaValores;
    }
}
