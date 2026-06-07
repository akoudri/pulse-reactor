package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class MonoSupplier {

    private static Faker faker = new Faker();

    public static Mono<String> getName() {
        System.out.println("Entering getName");
        //TODO: return a lazy Mono simulating a slow operation
        //step 1: use Mono.fromSupplier to wrap the (slow) generation of a name:
        //        - print "Generating name..."
        //        - sleep 5 seconds (Thread.sleep) to simulate a long computation
        //        - return faker.name().fullName()
        //step 2: map the result to upper case
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        //TODO: run the slow Mono without blocking the main thread
        //step 1: subscribe to getName() and print the result — how long is the main thread blocked?
        //step 2: move the work to another thread with subscribeOn(Schedulers.boundedElastic())
        //step 3: print "Bye" at the end of main — make sure the program lives long enough
        //        (Thread.sleep) to still see the generated name
        //question: in which order do "Entering getName", "Generating name..." and "Bye" appear? Why?
    }
}
