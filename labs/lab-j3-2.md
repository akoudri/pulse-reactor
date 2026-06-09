# Lab J3-2 — Temps réel, Kafka, résilience & observabilité

**Durée cible** : ~3 h · **Module** : `pulse-reactive` · **Solution de référence** : tag `lab-j3-2`
**Infra** : `docker compose --profile streaming --profile upstream up -d` (Kafka + WireMock) · **Point de départ** : tag `lab-j3-1`

## Objectif pédagogique

Transformer Pulse en système temps réel résilient et observable : les agents publient sur
Kafka, le pipeline pousse en SSE avec un backpressure réel, le fan-out survit aux pannes,
et un même `traceId` traverse Kafka → pipeline → SSE. On met en œuvre, au niveau système,
les patterns du deck du jour.

> **reactor-kafka est interdit** (discontinué) : on utilise Spring Kafka bridgé en `Flux`.

## Travail à réaliser

### Partie A — ingestion Kafka bridgée

Ajoutez `spring-kafka` à `pulse-reactive`. Créez un `@KafkaListener` (topic `metrics`) qui
**pousse** chaque message dans un `Sinks.Many<MetricSample>`
(`multicast().onBackpressureBuffer()`, borné). Le `sink.asFlux()` alimente le pipeline
d'ingestion existant (normalize/enrich des labs J1). Ajoutez un petit **producteur**
(KafkaTemplate) simulant des agents qui publient à intervalle. Écrivez des tests
d'intégration avec **Testcontainers Kafka**. Lancez `mvn verify`.
Interdit : `reactor-kafka`, `ReactiveKafkaConsumer/ProducerTemplate` (dépréciés).

### Partie B — temps réel de bout en bout + backpressure

Branchez le flux issu de Kafka sur le SSE `/api/metrics/stream` (réutilise J2-2). Simulez
un **client SSE lent** dans un test et montrez le comportement du pont : le `Sinks` borné
applique sa stratégie d'overflow quand l'aval ne suit pas. Documentez l'alternative
propre : mettre en **pause/resume** la consommation Kafka plutôt que de gonfler la mémoire
indéfiniment. Vérifiez qu'aucune file non bornée ne se forme.

### Partie C — résilience sur le fan-out

Sur `/api/health/aggregate` (de J2-2), ajoutez : un **circuit breaker** (natif SF7 sur
l'Interface Client, OU Resilience4j via `transformDeferred(CircuitBreakerOperator.of(...))`),
un **bulkhead** (concurrence bornée), et **un** exemple de **hedging**
(`Mono.firstWithSignal(call, call.delaySubscription(...))`) sur l'appel idempotent. Avec
l'upstream instable de WireMock, écrivez un test qui **provoque l'ouverture du circuit** et
vérifie le passage en fallback. Documentez le surcoût du hedging.

### Partie D — observabilité

Ajoutez `micrometer` (registry + `/actuator/prometheus`) et **Micrometer Tracing**.
Instrumentez : un `Counter` de taux d'ingestion, un `Timer` sur l'agrégation, un compteur
d'ouvertures de circuit. Vérifiez — grâce à la propagation de contexte activée en J3-1
(`Hooks.enableAutomaticContextPropagation()`) — qu'un **span / traceId** traverse la
frontière Kafka → pipeline → SSE et apparaît dans les logs corrélés.

## Critères d'acceptation

- [ ] `mvn verify` vert, **Testcontainers Kafka** pour l'intégration.
- [ ] Ingestion via `@KafkaListener` → `Sinks` → `Flux` ; **aucun** usage de `reactor-kafka` ni des templates réactifs dépréciés.
- [ ] Le pont `Sinks` est **borné** ; aucune file non bornée (vérifié sous client lent).
- [ ] Le circuit s'**ouvre** sous upstream instable et bascule en fallback (test à l'appui).
- [ ] `/actuator/prometheus` expose les métriques d'ingestion/agrégation.
- [ ] Un même `traceId` est corrélé de l'ingestion Kafka jusqu'au push SSE.

## Points à verbaliser

- Pourquoi Kafka offre-t-il un backpressure « naturel » (pull/poll), et où ce backpressure s'arrête-t-il (le pont `Sinks`) ?
- Quand le hedging est-il justifié, et pourquoi seulement sur des appels idempotents ?
- Sans la propagation de contexte de J3-1, qu'arrive-t-il au span à la frontière Kafka → pipeline ?
- Pourquoi un consumer Kafka bloquant sur virtual thread est-il une alternative crédible au pont réactif ?

## Pièges à éviter

- `reactor-kafka` ou `ReactiveKafka*Template` → Spring Kafka bridgé en `Flux`.
- `Sinks` / `onBackpressureBuffer` **non borné** → fuite mémoire sous aval lent.
- `retryWhen` ou hedging sur un appel **non idempotent** → doublons.
- Lire le `traceId` via `MDC`/`ThreadLocal` au fond du pipeline → propagation de contexte.
- Instrumenter avec des effets de bord bloquants dans le pipeline → sondes `doOn*` non bloquantes.

## Validation & solution de référence

- `mvn verify` vert et critères cochés.
- Solution complète au tag `lab-j3-2` (`git checkout lab-j3-2`).
