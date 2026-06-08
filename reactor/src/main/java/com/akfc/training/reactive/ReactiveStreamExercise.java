package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;

/**
 * Goal: implement a small reactive pipeline with the standard java.util.concurrent.Flow API
 * (no Reactor here!): publisher -> processor (transformation) -> subscriber.
 */
class EndSubscriber<T> implements Flow.Subscriber<T> {

    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        System.out.println("End subscriber received data " + item);
        System.out.println("Thread " + Thread.currentThread().getName());
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("Subscriber done");
    }
}

class TransformerProcessor<T, R> extends SubmissionPublisher<R> implements Flow.Processor<T, R> {

    private Flow.Subscription subscription;
    private Function<T, R> function;

    public TransformerProcessor(Function<T, R> function) {
        this.function = function;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        submit(function.apply(item));
        System.out.println("Thread " + Thread.currentThread().getName());
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
        close();
    }
}

public class ReactiveStreamExercise {
    public static void main(String[] argv) throws InterruptedException {
        /*SubmissionPublisher<Integer> publisher = new SubmissionPublisher<>();
        TransformerProcessor<Integer, Integer> transformerProcessor = new TransformerProcessor<>(e -> e * e);
        EndSubscriber<Integer> subscriber = new EndSubscriber<>();
        publisher.subscribe(transformerProcessor);
        transformerProcessor.subscribe(subscriber);
        for (int i = 0; i < 10; i++) {
            publisher.submit(i);
        }
        publisher.close();
        Thread.sleep(1000);*/
        Flux<Integer> f = Flux.range(1, 10)
                .delayElements(Duration.ofMillis(500))
                .map(e -> e * e);

        f.subscribe(System.out::println, System.err::println, () -> System.out.println("Processing Completed"));

        Thread.sleep(5000);
    }
}
