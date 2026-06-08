package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

public class FluxErrors {

    public static void main(String[] args) {
        Flux<Integer> f1 = Flux.range(1, 5);
        Flux.range(1, 20)
                .handle((data, sink) -> {
                    int v = data * data;
                    if (v == 100) sink.error(new RuntimeException("Forbidden value"));
                    else if (v % 2 == 0) sink.next(v);
                })
                //.onErrorReturn(100)
                //.onErrorComplete()
                //.onErrorResume(e -> f1)
                //.onErrorContinue((e, d) -> System.out.println(d + " has failed"))
                //.onErrorMap(e -> new RuntimeException("Not authorized"))
                .subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Flux completed"));
    }

}
