package com.akfc.training.reactive;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;

/**
 * Goal: implement a small reactive pipeline with the standard java.util.concurrent.Flow API
 * (no Reactor here!): publisher -> processor (transformation) -> subscriber.
 */
class EndSubscriber<T> implements Flow.Subscriber<T> {

    //TODO: keep a reference to the Flow.Subscription received in onSubscribe

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        //TODO: store the subscription and request the first element (subscription.request(1))
        //question: what happens if you never call request()? (backpressure!)
    }

    @Override
    public void onNext(T item) {
        //TODO: print the received item,
        //      simulate a slow consumer (Thread.sleep(1000)),
        //      then request the next element
    }

    @Override
    public void onError(Throwable throwable) {
        //TODO: print the error (printStackTrace)
    }

    @Override
    public void onComplete() {
        //TODO: print a completion message, e.g. "Subscriber done"
    }
}

class TransformerProcessor<T, R> extends SubmissionPublisher<R> implements Flow.Processor<T, R> {

    //TODO: a Processor is BOTH a Subscriber (of T) and a Publisher (of R).
    //      Extending SubmissionPublisher gives you the Publisher side for free:
    //      use submit(...) to push transformed items downstream.

    //TODO: keep a reference to the upstream Flow.Subscription
    //TODO: keep the transformation Function<T, R>, received in the constructor

    public TransformerProcessor(Function<T, R> function) {
        //TODO: store the function
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        //TODO: store the subscription and request the first element
    }

    @Override
    public void onNext(T item) {
        //TODO: apply the function to the item, submit() the result downstream,
        //      then request the next element from upstream
    }

    @Override
    public void onError(Throwable throwable) {
        //TODO: print the error
    }

    @Override
    public void onComplete() {
        //TODO: propagate completion downstream (hint: close() on SubmissionPublisher)
    }
}

public class ReactiveStreamExercise {
    public static void main(String[] argv) throws InterruptedException {
        //TODO: assemble the pipeline
        //step 1: create a SubmissionPublisher<Integer>
        //step 2: create a TransformerProcessor<Integer, Integer> squaring each value
        //step 3: create an EndSubscriber<Integer>
        //step 4: wire everything: publisher -> processor -> subscriber
        //step 5: submit the values 1..5 (e.g. every 500 ms), then close the publisher
        //step 6: keep the JVM alive long enough (Thread.sleep) to let the slow
        //        subscriber consume everything
        //question: the publisher emits faster than the subscriber consumes —
        //          where do the pending items wait, and what limits that buffer?
    }
}
