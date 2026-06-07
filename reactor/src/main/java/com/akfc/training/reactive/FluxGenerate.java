package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;

public class FluxGenerate {

    private static Faker faker = new Faker();

    public static void main(String[] args) throws InterruptedException {
        //step 1: tester les différentes méthodes de création de Flux
        //        (just, fromIterable, fromArray, range, ...)

        //step 2: generate a flux of integers using interval and square them
        //        hint: interval is asynchronous — keep the main thread alive to see the output

        //step 3: create an infinite flux using Flux.create(sink -> ...) emitting random
        //        full names (faker.name().fullName());
        //        complete the flux after the 17th element (sink.complete());
        //        buffer it by groups of 5 (buffer) and display the buffers
        //        question: how many elements does the last buffer contain?

        //step 4: create a stateful generator using Flux.generate to display
        //        the first 20 elements of the Fibonacci series
        //        hint: use the variant with a state supplier and a BiFunction —
        //        the state can be a Tuple2 (Tuples.of(0L, 1L)) holding the two
        //        previous values; emit the first one (sink.next) and return the new state;
        //        limit the infinite sequence with take(20)
    }
}
