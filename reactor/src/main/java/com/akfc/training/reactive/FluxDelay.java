package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

import java.time.Duration;

public class FluxDelay {

    public static void main(String[] args) throws InterruptedException {
        //TODO: emit the integers from 1 to 10, one element per second, and print each value
        //step 1: create a Flux emitting the range 1..10
        //step 2: delay each element by 1 second (have a look at delayElements)
        //step 3: subscribe and print each value
        //step 4: run the program. Why does nothing get printed?
        //        Hint: delayElements switches to another thread, and the main thread exits immediately.
        //step 5: block the main thread long enough (e.g. Thread.sleep) to let the Flux complete
    }
}
