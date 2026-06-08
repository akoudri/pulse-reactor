package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

import java.time.Duration;

public class FluxCreate {

    public static void main(String[] args) throws InterruptedException {
        Flux<Integer> f = Flux.just(1, 4, 7, 10, 12);
        //f.subscribe(System.out::println);
        Flux<Integer> g = Flux.from(f);
        //g.subscribe(System.out::println);
        /*Flux.empty()
                .subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Completed"));
        Flux.range(10, 10)
                .subscribe(System.out::println);*/
        Flux.interval(Duration.ofSeconds(1))
                .take(5)
                .subscribe(System.out::println);
        Thread.sleep(10000);
    }

}
