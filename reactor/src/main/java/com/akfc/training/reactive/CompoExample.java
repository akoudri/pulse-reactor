package com.akfc.training.reactive;

import reactor.core.publisher.Mono;

public class CompoExample {

    record PipelineState(String res1, String res2, String res3) {
        // Petit builder pratique pour faire évoluer l'état de manière immuable
        PipelineState withRes2(String r2) { return new PipelineState(res1, r2, res3); }
        PipelineState withRes3(String r3) { return new PipelineState(res1, res2, r3); }
    }

    public static void main(String[] args) {
        // Le pipeline d'exécution. L'état (PipelineState) est initialisé dès
        // la première étape, puis enrichi de façon immuable à chaque flatMap.
        Mono.just("1") // Étape 1 : produit res1
                .map(res1 -> new PipelineState(res1, null, null)) // Initialisation de l'état

                .flatMap(state -> callStep2(state.res1()) // Étape 2 : dépend de res1
                        .map(state::withRes2))   // On ajoute res2 à l'état

                .flatMap(state -> callStep3(state.res2()) // Étape 3 : dépend de res2
                        .map(state::withRes3))   // On ajoute res3 à l'état

                .doOnNext(state -> {
                    // L'état est plat, l'accès est direct et auto-documenté
                    System.out.println(state.res1());
                    System.out.println(state.res2());
                    System.out.println(state.res3());
                })
                .subscribe();
    }

    /**
     * Version simplifiée.
     *
     * Les trois Mono étant indépendants, Mono.zip est l'opérateur idiomatique :
     * il les combine directement en un Tuple3 PLAT (getT1, getT2, getT3) et
     * souscrit les sources en parallèle.
     */
    private static void solution() {

    }

    /**
     * Étape 2 : simule un appel asynchrone (accès BDD, service distant…)
     * qui dépend du résultat de l'étape 1.
     */
    private static Mono<String> callStep2(String res1) {
        return Mono.just(res1 + "2"); // produit res2 à partir de res1
    }

    /**
     * Étape 3 : simule un appel asynchrone qui dépend du résultat de l'étape 2.
     */
    private static Mono<String> callStep3(String res2) {
        return Mono.just(res2 + "3"); // produit res3 à partir de res2
    }

}
