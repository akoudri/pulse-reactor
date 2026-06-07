package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

public class FluxErrors {

    public static void main(String[] args) {
        //TODO: square the integers from 1 to 20, keep only the even squares, and handle a forbidden value
        //step 1: create a Flux emitting the range 1..20
        //step 2: use handle((value, sink) -> ...) to combine map + filter in one operator:
        //        - compute the square of each value
        //        - if the square equals 100, emit an error (sink.error) with a "Forbidden value" message
        //        - otherwise, emit the square only if it is even (sink.next)
        //step 3: recover from the error with onErrorReturn, replacing it with the value 100
        //step 4: subscribe with three callbacks: one for values, one for errors,
        //        and one printing "Processing completed" on completion
        //step 5: observe the output. Does the completion callback run? Why does the stream
        //        stop after the error even though we recovered from it?
    }

}
