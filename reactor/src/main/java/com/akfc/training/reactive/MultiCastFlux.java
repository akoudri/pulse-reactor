package com.akfc.training.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class MultiCastFlux {

    public static void main(String[] args) throws InterruptedException {
        Flux<Integer> f1 = Flux.range(1, 5).delayElements(Duration.ofMillis(500));
        Flux<Integer> f2 = Flux.range(6, 10).delayElements(Duration.ofMillis(300));
        f1.mergeWith(f2).subscribe(System.out::println);
        //Flux.merge(f1, f2).subscribe(System.out::println);
        //Flux.concat(f1, f2).subscribe(System.out::println);
//        Flux.zip(f2, f1)
//                .map(t -> t.getT1() * t.getT2())
//                .subscribe(System.out::println);
        Flux<Integer> f3 = Flux.range(1, 5);
//        f3.collectList().subscribe(System.out::println);
//        Mono<Integer> m1 = Mono.just(42);
//        Flux f4 = m1.flux();
//        Mono<Integer> m2 = f3.next();
//        Mono<Integer> m3 = f3.last();
//        Mono<Integer> m4 = f3.skip(2).next();
//        m4.subscribe(System.out::println);
//        Thread.sleep(5000);
    }

}
