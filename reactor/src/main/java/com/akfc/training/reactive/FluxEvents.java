package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

public class FluxEvents {

    public static void main(String[] args) {
        //TODO: observe the lifecycle events of a Flux with doOn* callbacks
        //step 1: create a Flux emitting the range 1..18
        //step 2: collect all elements into a list (collectList), then flatten the list
        //        back into a Flux (flatMapIterable). What did we gain/lose by doing this?
        //step 3: add a doOnNext callback printing "Processing <value>" for each element
        //step 4: add a doOnComplete callback printing "Finished"
        //step 5: subscribe and print each value; compare the order of the printed lines
    }
}
