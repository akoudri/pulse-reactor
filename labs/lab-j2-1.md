# Lab J2-1 — Schedulers, threads et backpressure réel

**Durée cible** : ~90 min · **Module** : `pulse-common` · **Solution de référence** : tag `lab-j2-1`
**Infra** : aucune (flux purs) · **Point de départ** : tag `lab-j1-2`

## Objectif pédagogique

Voir *concrètement* sur quel thread s'exécute chaque étape (`publishOn` vs `subscribeOn`),
offload correctement du bloquant (`boundedElastic`) sans abuser de `parallel`, puis
soumettre le pipeline d'ingestion à un **producteur rapide / consommateur lent** pour
observer le backpressure et choisir une stratégie. C'est ici qu'on traite le backpressure
en profondeur — maintenant qu'il y a une vraie frontière asynchrone.

## Travail à réaliser

### Partie A — threads

Créez un package `reactor.playground` dans `pulse-common` avec une classe `SchedulersDemo`
exposant des méthodes qui loggent le nom du thread à chaque étape (`log()` ou `doOnNext`
avec `Thread.currentThread().getName()`), pour illustrer : (1) un pipeline sans scheduler,
(2) l'effet de `subscribeOn(boundedElastic)`, (3) l'effet de `publishOn(parallel)` placé au
milieu du pipeline, (4) la combinaison des deux. Ajoutez un test `SchedulersDemoTest` qui
**capture** les noms de threads (via un `doOnNext` qui pousse dans une collection
concurrente) et **asserte** que l'étape avant et après un `publishOn` ne tournent pas sur
le même pool.

### Partie B — offload de bloquant

Ajoutez une méthode qui enveloppe un appel bloquant simulé (`Thread.sleep` dans un
`Mono.fromCallable`) correctement sur `boundedElastic` via `subscribeOn`. Documentez
pourquoi `parallel()` serait un mauvais choix ici. Ajoutez un test « BlockHound-friendly »
(ce blocage est *attendu* car isolé sur `boundedElastic` — notez-le en commentaire ;
BlockHound arrivera en J4).

### Partie C — backpressure

Créez `IngestionBackpressure` : un producteur rapide (`Flux.interval(Duration.ofMillis(1))`
mappé en `MetricSample`) consommé par une étape lente (latence simulée). Exposez trois
variantes paramétrées par stratégie : `onBackpressureBuffer`, `onBackpressureLatest`,
`onBackpressureDrop`. Chaque variante publie sur `boundedElastic` le consommateur lent.
Écrivez `IngestionBackpressureTest` qui, avec **`StepVerifier.create(flux, 0)`** (demande
initiale nulle) puis des `thenRequest(n)`, démontre le **protocole de demande** : aucune
donnée n'arrive avant `thenRequest`, puis exactement `n` éléments.

### Partie D — debug

Ajoutez un `checkpoint("ingestion")` dans le pipeline et montrez, en commentaire d'en-tête
de classe, comment activer le `ReactorDebugAgent` (instrumentation, coût négligeable) et
pourquoi on ne met **pas** `Hooks.onOperatorDebug()` en production (coût).

Lancez `mvn verify`.

## Critères d'acceptation

- [ ] `mvn verify` vert.
- [ ] Le test de la Partie A **prouve** par assertion le changement de pool autour d'un
      `publishOn` (pas juste des logs).
- [ ] Aucun usage de `Schedulers.elastic()` (supprimé) ; offload du bloquant sur
      `boundedElastic`, pas sur `parallel`.
- [ ] La Partie C démontre le backpressure via `StepVerifier` à demande initiale 0 +
      `thenRequest`, et les trois stratégies sont distinctes et commentées.
- [ ] `checkpoint()` présent ; commentaire sur `ReactorDebugAgent` vs `Hooks.onOperatorDebug()`.

## Points à verbaliser

- `subscribeOn` agit sur **toute la chaîne en amont** quel que soit son emplacement ;
  `publishOn` ne déplace que **l'aval** de son point d'insertion. Le démontrer sur les logs.
- Pourquoi `parallel()` est fait pour du CPU-bound non bloquant et `boundedElastic` pour
  isoler du bloquant ? Que se passe-t-il si on bloque sur le pool `parallel` ?
- **Aside Loom (à planter, pas à développer)** : `boundedElastic` existe pour offloader du
  bloquant ; avec les virtual threads on peut parfois éviter cet offload — mais Reactor
  n'exécute pas ses opérateurs sur des virtual threads par défaut. On mesurera le vrai
  arbitrage en J2-2 et J4 avec le jumeau MVC+Loom.
- Backpressure = *pull* : le consommateur demande (`request(n)`), le producteur ne pousse
  pas plus que demandé. Les stratégies `onBackpressure*` ne servent que quand le producteur
  ne **peut pas** ralentir (ex. `interval`, sources hot).

## Pièges à éviter

- « Paralléliser pour aller plus vite » via `parallel()` sur un pipeline I/O → non,
  c'est `flatMap` + `boundedElastic` et une concurrence bornée.
- Empiler `publishOn` un peu partout « au cas où » → chaque changement de thread a un coût.
- Tester le backpressure avec des `sleep` et des compteurs → non, `StepVerifier` à
  demande contrôlée le montre de façon déterministe.

## Validation & solution de référence

- `mvn verify` vert et critères cochés.
- Solution complète au tag `lab-j2-1` (`git checkout lab-j2-1`).
