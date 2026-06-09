# Lab J3-1 — R2DBC & propagation de contexte

**Durée cible** : ~2 h 30 · **Module** : `pulse-reactive` · **Solution de référence** : tag `lab-j3-1`
**Infra** : `docker compose --profile data up -d` (PostgreSQL) · **Point de départ** : tag `lab-j2-2`

## Objectif pédagogique

Faire passer Pulse du stockage en mémoire à PostgreSQL en R2DBC, puis traiter le sujet le
plus subtil du réactif : **propager un contexte** (traceId) de l'entrée HTTP jusqu'au log
de la requête SQL, à travers les changements de thread — d'abord à la main (Context Reactor),
puis automatiquement (Micrometer context-propagation).

## Travail à réaliser

### Partie A — persistance R2DBC

Dans `pulse-reactive`, ajoutez `spring-boot-starter-data-r2dbc` et le driver
`r2dbc-postgresql` (versions gérées par le BOM Boot). Configurez `spring.r2dbc.*` vers la
base `pulse` (docker `--profile data`, port 5432). Créez un `schema.sql` (table `alert`).
Définissez une entité `Alert` (`@Table`, `@Id`) — réutilisez le record de `pulse-common` si
possible. Créez `AlertRepository extends R2dbcRepository<Alert, Long>` avec une requête
dérivée `findByMetric`. **Remplacez** l'`AlertService` en mémoire de J2-2 par le repository.
Écrivez des tests d'intégration avec **Testcontainers** (PostgreSQL). Lancez `mvn verify`.

### Partie B — DatabaseClient (une fois)

Ajoutez un endpoint d'agrégat (`GET /api/alerts/by-metric`) servi par une requête
`DatabaseClient.sql(...)` (GROUP BY) renvoyant un `Flux`. Objectif : montrer le requêtage
fluide quand le repository ne suffit pas, et que tout reste `Mono`/`Flux`.

### Partie C — transaction réactive

Ajoutez `createWithAudit(AlertRule)` : sauvegarde l'alerte ET une ligne d'audit dans la
même transaction (`@Transactional` sur la méthode renvoyant `Mono<Alert>`). Écrivez un test
qui force l'échec de l'audit et **vérifie le rollback** (l'alerte n'est pas persistée).

### Partie D — propagation de contexte (le cœur)

1. Créez un `WebFilter` qui, à l'entrée, génère un `traceId`, le pose dans le **Context**
   (`contextWrite`) et dans le **MDC**.
2. Activez la propagation automatique : `Hooks.enableAutomaticContextPropagation()` au
   démarrage, et ajoutez la dépendance Micrometer **context-propagation**.
3. Loggez à l'entrée du contrôleur ET juste avant/après l'accès R2DBC. Le `traceId` doit
   apparaître **dans les deux logs**, malgré les changements de thread.
4. En complément manuel : lisez une clé `tenant` via `Mono.deferContextual` au fond du
   pipeline, pour illustrer la lecture explicite du Context.

Test : un appel HTTP avec un header `X-Trace-Id` se retrouve corrélé du filtre au log SQL.

## Critères d'acceptation

- [ ] `mvn verify` vert, **Testcontainers** PostgreSQL pour les tests d'intégration (pas de base partagée).
- [ ] Aucun driver/appel **bloquant** (JDBC) ; tout passe par R2DBC / `DatabaseClient`.
- [ ] La transaction de la Partie C **rollback** prouvé par test en cas d'échec d'audit.
- [ ] Le **même `traceId`** apparaît dans le log d'entrée et dans le log d'accès données.
- [ ] Aucune lecture de `ThreadLocal` nu au fond du pipeline pour le contexte transverse.
- [ ] `Hooks.enableAutomaticContextPropagation()` appelé une fois au démarrage.

## Points à verbaliser

- Pourquoi, **sans** la propagation, le `traceId` du MDC disparaît-il entre le filtre et le log SQL ? (changement de thread, `ThreadLocal` perdu).
- Dans quel sens circule le `Context` — et pourquoi écrit-on le `contextWrite` dans le `WebFilter` (près de la souscription) ?
- Qu'est-ce que R2DBC ne fait PAS qu'Hibernate ferait ? (dirty checking, lazy, cache L1) — et quand ça pousse vers JDBC+Loom.
- Sur `/api/metrics/stream` branché à la base : où agit le backpressure du client jusqu'au curseur ?

## Pièges à éviter

- Tirer un driver **JDBC** ou faire un appel bloquant « juste pour cette requête » → R2DBC partout.
- Lire `MDC.get(...)` ou `SecurityContextHolder` directement au fond du pipeline → passer par le Context / la propagation automatique.
- `block()` pour « simplifier » un test d'intégration → `StepVerifier` / `WebTestClient`.
- Base de test partagée / `schema.sql` joué sur une vraie base → Testcontainers, éphémère.
- Mettre des données métier dans le Context → uniquement du transverse (trace, tenant, sécurité).

## Validation & solution de référence

- `mvn verify` vert et critères cochés.
- Solution complète au tag `lab-j3-1` (`git checkout lab-j3-1`).
