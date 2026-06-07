package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

public class MultiCastFlux {

    public static void main(String[] args) throws InterruptedException {
        //TODO: explore multicasting and combination operators
        //step 1: create a Flux emitting 0..9 with a 1-second delay between elements,
        //        and make it "hot" with share();
        //        subscribe a first consumer, wait 3 seconds (Thread.sleep),
        //        then subscribe a second consumer;
        //        question: which values does the second subscriber receive?
        //        what happens without share()?

        //step 2: create two fluxes f1 (range 1..5) and f2 (range 6..10)

        //step 3: combine f1 and f2 with concatWith and print the result

        //step 4: combine f1 and f2 with zipWith, multiplying the paired elements

        //step 5: zip the three fluxes f1, f2 and f1 together with Flux.zip and print the tuples

        //step 6: on f1, chain doOnNext / materialize / doOnNext / dematerialize and subscribe;
        //        compare the two doOnNext outputs — what does materialize turn the
        //        values and signals (onNext, onComplete) into?
    }

}
