package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

public class FluxEvents {

    public static void main(String[] args) {
        /*Flux.range(1, 50)
                .map(e -> e * e)
                //.doOnNext(e -> System.out.println("Valeur sortie du map = " + e))
                .doOnComplete(() -> System.out.println("Map Completed"))
                .doOnCancel(() -> System.out.println("Map cancelled"))
                .filter(e -> e % 2 == 0)
                .doOnCancel(() -> System.out.println("Filter cancelled"))
                .take(20)
                .subscribe(System.out::println);*/

        Flux.range(1, 50)
                .handle((data, sink) -> {
                    int v = data * data;
                    if (v % 2 == 0) sink.next(v);
                }).subscribe(System.out::println);
    }
}
