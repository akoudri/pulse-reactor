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
        return Mono.just(getFullName());
        //return Mono.fromSupplier(MonoCreate::getFullName);
    }

    public static void main(String[] args) throws InterruptedException {
        Mono<String> m = fullName();
        Thread.sleep(3000);
        m.subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Processing Completed"));
        /*String message = fullName().block();
        System.out.println(message);*/
    }

}
