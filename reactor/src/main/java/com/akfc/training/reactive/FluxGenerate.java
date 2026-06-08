package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

import java.util.Random;

public class FluxGenerate {

    private static Faker faker = new Faker();
    private static Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        /*Flux.create(sink -> {
            for (int i = 0; i < 17; i++) {
                sink.next(i);
            }
            sink.complete();
        }).subscribe(System.out::println);*/
        /*Flux.generate(sink -> {
            sink.next(FluxGenerate.random.nextInt(100));
        })
                .take(10)
                .subscribe(System.out::println);*/
        Flux.generate(
                        () -> Tuples.of(0L, 1L),
                        (state, sink) -> {
                            long a = state.getT1();
                            long b = state.getT2();
                            sink.next(a);
                            return Tuples.of(b, a + b);
                        }
                )
                .take(20)
                .subscribe(System.out::println);
    }
}
