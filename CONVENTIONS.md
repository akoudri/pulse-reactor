# CONVENTIONS.md — Projet Pulse

> Guide de conventions pour le code écrit dans Pulse, tout au long de la formation.
> Objectif : un code cohérent d'un lab à l'autre, à l'état de l'art 2026. À garder
> ouvert pendant les exercices.

## 1. Ce qu'est Pulse

Pulse est un **agrégateur de télémétrie / supervision**, fil rouge d'une formation Spring
Reactor de 4 jours. Il grandit lab par lab (voir `labs/LAB-PLAN.md`).

Deux implémentations du **même domaine**, maintenues en parallèle :

- `pulse-reactive` : WebFlux + R2DBC + Reactor — la colonne vertébrale réactive.
- `pulse-mvc-loom` : Spring MVC + **virtual threads** + JDBC/JPA bloquant — le **jumeau**.

Le jumeau n'est pas du legacy : c'est le **cas différentiel** assumé. Sur le CRUD
I/O-bound (règles d'alerte, dashboards), on montre que MVC+Loom suffit ; sur
l'ingestion/streaming, on montre que le réactif gagne. Les deux exposent le **même
contrat d'API** (DTOs dans `pulse-common`).

## 2. Stack & versions (à respecter)

- **Java 21** au minimum (idéalement 25). Le différentiel Loom *exige* les virtual
  threads GA de Java 21 — non négociable, même si Boot 4 tolère Java 17.
- **Spring Boot 4.0.x** (dernier GA). Pas la 4.1 RC.
- **Reactor 2025.0** : géré par le BOM Boot ; ne pas pinner `reactor-core` à la main
  (il reste en 3.8.x sous ce train).
- **Jackson 3** (défaut Boot 4), **Jakarta EE 11**, null-safety **JSpecify**.
- Versions gérées par le **BOM Spring Boot** importé dans le parent. On ne pinne pas
  les versions des dépendances Spring/Reactor/Testcontainers individuellement.

> En cas de doute sur un coordinate Maven Boot 4 (nom de starter, starter de test
> granulaire), vérifiez-le dans la **documentation officielle Spring Boot 4 / Reactor** —
> n'inventez pas un artefact. Si un starter de test granulaire vous échappe,
> `spring-boot-starter-test-classic` ramène un classpath de test complet.

## 3. Structure du repo (Maven multi-modules)

```
pulse/
├── pom.xml                # parent : import BOM Boot 4, <release>21</release>, modules
├── pulse-common/          # DTOs d'API + contrats partagés (PAS d'entités persistées)
├── pulse-reactive/        # WebFlux + Spring Data R2DBC + Reactor
├── pulse-mvc-loom/        # spring-boot-starter-web-mvc + virtual threads + JDBC/JPA
├── pulse-loadtest/        # scénarios de charge (Gatling) pour le différentiel
├── docker-compose.yml
├── CONVENTIONS.md
└── labs/                  # énoncés de lab (un .md par checkpoint)
```

- `pulse-common` ne contient que des records DTO + interfaces de contrat. Chaque app a
  son propre modèle de persistance (entités R2DBC d'un côté, JPA de l'autre).
- Starters : `spring-boot-starter-webflux` (réactif), `spring-boot-starter-web-mvc`
  (jumeau). Pour un client HTTP sans serveur, le module `spring-boot-webclient`.

## 4. Règles Reactor — état de l'art 2026 (à appliquer systématiquement)

- **API Sinks** (`Sinks.many()`, `Sinks.one()`) pour les sources programmatiques.
  **Jamais** `Processor`/`DirectProcessor`/`EmitterProcessor` (dépréciés).
- Offload de bloquant : **`Schedulers.boundedElastic()`**. **Jamais** `Schedulers.elastic()`
  (supprimé). `parallel()` = CPU-bound non bloquant ; `single()` = faible latence.
- Retry : **`retryWhen(Retry.backoff(maxAttempts, minBackoff))`** (`reactor.util.retry.Retry`).
  **Jamais** `retryBackoff` (supprimé).
- Backpressure : `onBackpressureBuffer` / `onBackpressureDrop` / `onBackpressureLatest`
  / `onBackpressureError`. (« onBackpressureLast » n'existe pas.)
- **`flatMap`** quand l'ordre n'importe pas + concurrence ; **`concatMap`** pour
  préserver l'ordre. Documentez le choix en commentaire quand il est subtil.
- **Contexte** : `contextWrite` / `deferContextual` + lib Micrometer **context-propagation**
  et `Hooks.enableAutomaticContextPropagation()` au démarrage. Pas de `ThreadLocal` nu
  (MDC, SecurityContext) traversant une frontière réactive.
- Gardez en tête assembly-time vs subscription-time : rien ne s'exécute tant qu'on ne
  souscrit pas.

## 5. Le jumeau MVC+Loom

- `spring.threads.virtual.enabled=true` ; serveur servlet (Tomcat) thread-per-request
  sur virtual threads.
- Accès données bloquant assumé (JDBC / Spring Data JPA), code impératif lisible.
- Mêmes endpoints, mêmes DTOs (`pulse-common`) que `pulse-reactive`, pour que le
  load-test compare deux implémentations strictement équivalentes fonctionnellement.

## 6. Conventions de test

- Flux/Mono : **StepVerifier**, avec **`StepVerifier.withVirtualTime`** pour tout ce qui
  dépend du temps (delays, intervals, timeouts). Pas de `Thread.sleep` dans les tests.
- Web : **WebTestClient** (réactif), MockMvc côté jumeau.
- Intégration : **Testcontainers** (Postgres, Kafka) — gérés par le BOM Boot.
- **BlockHound** en scope test uniquement, pour détecter les appels bloquants dans
  `pulse-reactive`. Épinglez une version alignée sur le JDK utilisé (vérifiez la
  compatibilité JDK 21/25 ; BlockHound dépend de la version de la JVM).
- Lancez `mvn verify` **avant** de considérer un checkpoint atteint.

## 7. Intégrations

- **Kafka** : `spring-kafka` (le client Java est bloquant par nature) **bridgé vers
  `Flux`**. **Jamais** `reactor-kafka` (projet discontinué en mai 2025, hors du BOM
  Reactor 2025.0).
- **Résilience** : privilégier le natif Spring Framework 7 (`@Retryable` sur retour
  réactif, circuit breaker des Interface Clients) ; Resilience4j pour bulkhead /
  rate-limiter avancés. Toujours peser les deux, ne pas défaulter à Resilience4j.

## 8. Ce qu'on ne fait JAMAIS dans le code applicatif

- `block()` / `blockFirst()` / `blockLast()` dans un pipeline ou un contrôleur
  (toléré uniquement dans un `main` de démo console, et explicitement étiqueté comme tel).
- `subscribe()` « sauvage » dans un contrôleur (on retourne le Publisher au framework).
- `parallel()` par réflexe sans mesure (c'est pour le CPU-bound, pas l'I/O).
- `Schedulers.elastic()`, `Processor`, `retryBackoff`, `reactor-kafka` (tous obsolètes).

## 9. Discipline de travail (par lab)

1. Lire l'énoncé du lab dans `labs/`.
2. Écrire le code dans le **module concerné** uniquement (sauf mention contraire).
3. Écrire les tests (StepVerifier / WebTestClient / Testcontainers).
4. Lancer `mvn verify`.
5. Vérifier les critères d'acceptation, puis se comparer à la **solution de référence**
   (le tag `lab-jX-Y`) — après avoir cherché par soi-même.
