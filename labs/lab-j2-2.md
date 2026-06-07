# Lab J2-2 — API WebFlux, jumeau MVC+Loom & différentiel Gatling

**Durée cible** : ~3 h (toute la PM) · **Modules** : `pulse-reactive`, `pulse-mvc-loom`, `pulse-loadtest`
**Solution de référence** : tag `lab-j2-2` · **Infra** : `docker compose --profile upstream up -d` · **Point de départ** : tag `lab-j2-1`

## Objectif pédagogique

Exposer Pulse en API réactive, l'alimenter par un fan-out WebClient et un flux SSE, puis
**construire son jumeau MVC+Loom** à contrat identique et les **départager à la charge**
avec Gatling. C'est le cœur du dispositif différentiel : à la fin, on a des chiffres, pas
une opinion. Pas de base de données ici (service en mémoire) — la persistance R2DBC arrive
en J3.

> Lab volumineux : le **checkpoint minimal** = Parties A→D fonctionnelles + une première
> exécution Gatling (Partie E). Le réglage fin de Gatling peut déborder en J4 si besoin.

## Travail à réaliser

### Partie A — l'API réactive (`pulse-reactive`)

Créez le module `pulse-reactive` : appli Spring Boot 4, `spring-boot-starter-webflux`,
port 8080. Implémentez un service `AlertService` **en mémoire** (Map concurrente) exposant
`Flux<Alert> all()`, `Mono<Alert> byId(String)`, `Mono<Alert> create(AlertRule)`. Réutilisez
les DTOs de `pulse-common` (records). Écrivez un contrôleur annoté `@RestController` sous
`/api/alerts` (GET liste, GET /{id}, POST avec `@Valid @RequestBody`). Ajoutez une route en
**style fonctionnel** (`RouterFunction`) pour `/api/alerts/count`, afin d'illustrer les deux
styles. Aucun `block()` : on retourne les Publishers. Écrivez des tests `WebTestClient`.
Lancez `mvn verify`.

### Partie B — fan-out WebClient vers les upstreams

Créez des stubs WireMock dans `./upstream-sim/` (mappings) simulant 3 endpoints `/health` :
un rapide, un lent (latence ~700 ms), un instable (50 % de 503). Dans `pulse-reactive`,
ajoutez `GET /api/health/aggregate` qui interroge les 3 upstreams **en parallèle** via
`WebClient` (fan-out `flatMap` à concurrence bornée), avec `timeout(800ms)` +
`retryWhen(Retry.backoff(...))` + `onErrorReturn(DOWN)` par appel, puis agrège en
`Mono<AggregateHealth>`. Le `WebClient` cible `http://localhost:8089` (upstream-sim).
Testez avec un `WebClient` pointant sur WireMock. Lancez `mvn verify`.

### Partie C — flux temps réel (SSE)

Branchez la `Sinks.Many` d'ingestion du lab J2-1 sur un `GET /api/metrics/stream`
produisant `text/event-stream` : `Flux<ServerSentEvent<MetricSample>>`. Un petit simulateur
d'agents pousse des échantillons dans le sink à intervalle. Vérifiez le flux avec
`WebTestClient` (`returnResult` + `StepVerifier` sur le corps). Lancez `mvn verify`.

### Partie D — le jumeau MVC+Loom (`pulse-mvc-loom`)

Créez `pulse-mvc-loom` : appli Spring Boot 4, **`spring-boot-starter-web-mvc`**, port 8081,
`spring.threads.virtual.enabled=true`. Réimplémentez **exactement les mêmes endpoints et
DTOs** que `pulse-reactive`, en style impératif bloquant :
- `AlertService` en mémoire renvoyant `List<Alert>` / `Alert` ;
- `/api/health/aggregate` via **`RestClient`** (bloquant) — appels séquentiels OU
  `CompletableFuture` sur l'executor virtuel, au choix, à documenter ;
- `/api/metrics/stream` via `SseEmitter` (l'équivalent MVC honnête du SSE).

Pas de WebFlux ici. Écrivez des tests MockMvc. Lancez `mvn verify`.

### Partie E — le différentiel Gatling (`pulse-loadtest`)

Créez `pulse-loadtest` avec le **gatling-maven-plugin** et une simulation en **DSL Java**
(`io.gatling.javaapi`). (Vérifiez la version courante du plugin dans la documentation
officielle Gatling.) Scénario : montée en charge (`rampUsersPerSec`) sur
`/api/health/aggregate` (le cas fan-out, le plus discriminant). `baseUrl` paramétrable par
propriété système pour viser 8080 (réactif) puis 8081 (jumeau). Mesurez débit, p99, et
conservez les rapports HTML. Documentez comment lancer les deux runs et où lire la
comparaison.

## Critères d'acceptation

- [ ] `pulse-reactive` (8080) et `pulse-mvc-loom` (8081) exposent des endpoints **identiques** (mêmes chemins, mêmes DTOs `pulse-common`).
- [ ] Réactif : `WebClient` partout, aucun `block()`/`subscribe()` dans le code applicatif ; jumeau : `RestClient` + virtual threads activés.
- [ ] `/api/health/aggregate` survit à l'upstream lent et à l'instable (timeout + retry + repli), des deux côtés.
- [ ] Le SSE émet bien un `text/event-stream` continu (réactif) / `SseEmitter` (jumeau).
- [ ] Gatling tourne contre les deux apps et produit deux rapports comparables.
- [ ] `mvn verify` vert sur les trois modules.

## Points à faire verbaliser

- Sur le fan-out, qu'observe-t-on entre les deux implémentations quand l'upstream lent répond en 700 ms et que la charge monte ? (le réactif ne consomme pas un thread par requête en attente).
- Le jumeau avec virtual threads tient-il le CRUD aussi bien que le réactif ? (souvent oui — c'est le point honnête).
- Sur le SSE, pourquoi le modèle réactif est-il structurellement plus à l'aise que `SseEmitter` + thread ?
- Que mesure-t-on vraiment : débit et p99 **sous charge**, pas la latence d'une requête seule.

## Pièges à éviter

- `block()` ou `subscribe()` dans un contrôleur réactif → on retourne le Publisher.
- `RestTemplate` côté jumeau → `RestClient` (l'API moderne, compatible virtual threads).
- `flatMap` du fan-out sans borne de concurrence → on protège les upstreams.
- Les deux apps sur le même port, ou des contrats divergents → la comparaison ne vaut plus rien.
- Conclure « le réactif est plus rapide » sur une requête isolée → la latence unitaire n'est pas le sujet.

## Validation & solution de référence

- `mvn verify` vert sur les trois modules ; les deux apps répondent et Gatling produit ses rapports.
- Solution complète au tag `lab-j2-2` (`git checkout lab-j2-2`).
