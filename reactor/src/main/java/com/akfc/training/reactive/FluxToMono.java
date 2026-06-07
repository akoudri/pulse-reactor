package com.akfc.training.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FluxToMono {

    public static void main(String[] args) {
        Flux f = Flux.range(0, 10);
        //TODO: transform the flux into a mono, two different ways
        //step 1: build a Mono holding only the first element of the flux
        //        hint: look at next() (what is the difference with blockFirst()?)
        //step 2: build a Mono holding ALL the elements as a single list
        //        hint: look at collectList()
        //step 3: subscribe to both monos and print the results to check your answers
    }
}
