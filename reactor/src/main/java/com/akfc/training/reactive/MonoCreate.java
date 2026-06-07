package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Mono;

public class MonoCreate {

    private static Faker faker = new Faker();

    public static String getFullName() {
        System.out.println("Generating name...");
        return faker.name().fullName();
    }

    public static Mono<String> fullName() {
        //Step 1: create the Mono with Mono.just(getFullName()) and run the program.
        //        When is "Generating name..." printed: at assembly time or at subscription time?
        //Step 2: now create the Mono with a supplier (Mono.fromSupplier and a method reference
        //        to getFullName). Run again and compare: which version is lazy?
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        //TODO: build the processing pipeline
        //step 1: call fullName() and add a log() to trace the reactive signals
        //step 2: map the name to upper case, then add another log() — observe where
        //        each signal travels in the chain
        //step 3: subscribe with three callbacks: print the value, print the stack trace
        //        on error, and print "Processing completed" on completion
    }

}
