package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Random;

public class FluxFlatMap {

    private static Faker faker = new Faker();
    private static Random random = new Random();

    public static Flux<String> favoriteAnimals(String username) {
        return Flux.range(1, 5)
                .delayElements(Duration.ofMillis(random.nextInt(500) + 100))
                .map(i -> faker.animal().name() + " (" + username + ")");
    }

    public static Flux<String> users() {
        return Flux.range(1, 10)
                .map(i -> faker.name().fullName());
    }

    public static void main(String[] args) throws InterruptedException {
        users()
                //.parallel()
                //.runOn(Schedulers.parallel())
                .flatMap(FluxFlatMap::favoriteAnimals)
                .doOnNext(data -> System.out.println(Thread.currentThread().getName()))
                .subscribeOn(Schedulers.boundedElastic())
                .map(String::toUpperCase)
                .log()
                .publishOn(Schedulers.parallel())
                .doOnNext(data -> System.out.println(Thread.currentThread().getName()))
                //.sequential()
                .subscribe(System.out::println);
        /*users()
                .concatMap(FluxFlatMap::favoriteAnimals)
                .subscribe(System.out::println);*/
        Thread.sleep(10000);
    }

}
