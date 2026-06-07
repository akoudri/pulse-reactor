package com.akfc.training.reactive;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;

public class FluxFlatMap {

    private static Faker faker = new Faker();

    public static Flux<String> favoriteAnimals(String username) {
        //TODO: return a Flux of 5 random animal names for the given user
        //hint: Flux.range + map with faker.animal().name()
        return null;
    }

    public static Flux<String> users() {
        //TODO: return a Flux of 5 random user names
        //hint: Flux.range + map with faker.name().fullName()
        return null;
    }

    public static void main(String[] args) {
        //TODO: for each user, fetch their favorite animals and print them
        //step 1: start from users()
        //step 2: for each user, call favoriteAnimals(user) — which operator turns
        //        each element into an inner publisher and merges the results? (flatMap)
        //step 3: subscribe and print each animal name
        //step 4: what would happen with map instead of flatMap? Try it and look at the type
    }

}
