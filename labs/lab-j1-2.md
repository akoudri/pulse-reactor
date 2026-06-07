# Lab J1-2 — Opérateurs, gestion d'erreurs, test en temps virtuel

**Durée cible** : ~90 min · **Module** : `pulse-common` · **Solution de référence** : tag `lab-j1-2`
**Infra** : aucune (flux purs) · **Point de départ** : tag `lab-j1-1`

## Objectif pédagogique

Enrichir le flux d'ingestion par un appel **asynchrone potentiellement faillible**, et
durcir le pipeline avec les opérateurs de gestion d'erreurs réactifs. Puis tester tout
ça — y compris les délais de back-off — **en temps virtuel**, donc instantanément.

Trois acquis : (1) choisir `flatMap`/`concatMap` en conscience, (2) traiter l'erreur
comme un signal (`onErrorResume`, `retryWhen(Retry.backoff)`, `timeout`), (3) tester un
flux temporel sans attendre l'horloge réelle.

## Travail à réaliser

On part de l'état du lab précédent (record `MetricSample`, classe `IngestionPipeline`
avec `normalize`).

1. Dans `pulse-common`, ajoutez :
   - un record `EnrichedSample(MetricSample sample, String region)` ;
   - une interface contrat `AgentDirectory { Mono<String> regionOf(String agentId); }`
     (simule un annuaire distant ; l'implémentation réelle viendra plus tard) ;
   - une implémentation de test `FlakyAgentDirectory` qui échoue les N premiers appels
     (paramétrable) puis renvoie une région, avec une latence simulée via `Mono.delay`.
2. Ajoutez à `IngestionPipeline` la méthode
   `Flux<EnrichedSample> enrich(Flux<MetricSample> normalized, AgentDirectory directory)`
   qui, pour chaque échantillon, appelle `directory.regionOf(...)` et l'assemble en
   `EnrichedSample`. L'appel doit être protégé par :
   - `retryWhen(Retry.backoff(3, Duration.ofMillis(200)))` ;
   - un `timeout(Duration.ofSeconds(2))` ;
   - un repli `onErrorReturn("unknown")` en dernier recours.
3. Choisissez `flatMap` (l'ordre des échantillons enrichis n'est pas significatif) et
   **commentez** pourquoi pas `concatMap`. Limitez la concurrence de `flatMap` (paramètre
   de concurrence explicite, pas l'illimité par défaut).
4. Écrivez `IngestionPipelineEnrichTest` avec **`StepVerifier.withVirtualTime`** :
   - cas nominal (région résolue) ;
   - cas où l'annuaire échoue 2 fois puis réussit → vérifiez via `thenAwait` que le
     back-off est traversé et que l'enrichissement aboutit ;
   - cas où l'annuaire échoue au-delà du retry → repli `"unknown"`.
5. Lancez `mvn verify`.

## Critères d'acceptation

- [ ] `mvn verify` vert.
- [ ] `enrich` utilise `flatMap` avec **concurrence bornée** et un commentaire justifiant
      le choix `flatMap` vs `concatMap`.
- [ ] Retry via **`retryWhen(Retry.backoff(...))`** — surtout pas `retryBackoff` ni un
      `retry(long)` avec délais manuels.
- [ ] Le test utilise `StepVerifier.withVirtualTime` + `thenAwait`, **aucun**
      `Thread.sleep`, et le test s'exécute en quelques ms malgré les back-off de 200 ms.
- [ ] Aucun `block()` / `subscribe()` dans le code de production.

## Points à faire verbaliser

- Pourquoi `withVirtualTime` prend un **`Supplier<Publisher>`** et pas un flux déjà
  construit ? (le scheduler virtuel doit exister *avant* l'assemblage du flux temporel).
- Que se passe-t-il si on remplace `flatMap` par `concatMap` ? (sérialisation des appels
  → latence cumulée ; quel impact mesurable plus tard).
- `onErrorReturn` vs `onErrorResume` : quand préférer l'un à l'autre ?
- Le `timeout` agit-il par échantillon ou sur le flux global ici ? Pourquoi ça compte.

## Pièges à éviter

- `flatMap` sans borne de concurrence sur un appel distant → risque d'écrouler l'annuaire
  (on en reparle au backpressure J2-1).
- Récupérer l'erreur avec un `try/catch` autour d'un `.block()` → non, l'erreur est un
  signal, on la traite dans le pipeline.
- Test qui « attend vraiment » 600 ms de back-off → non, temps virtuel.

## Validation & solution de référence

- `mvn verify` vert et critères cochés.
- Solution complète au tag `lab-j1-2` (`git checkout lab-j1-2`).
