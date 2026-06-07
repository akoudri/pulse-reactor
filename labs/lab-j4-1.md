# Lab J4-1 — Tests réactifs, BlockHound & CI

**Durée cible** : ~2 h 30 · **Module** : `pulse-reactive` (+ CI au niveau repo) · **Solution de référence** : tag `lab-j4-1`
**Infra** : Docker requis (Testcontainers gère ses propres conteneurs — pas de profil compose) · **Point de départ** : tag `lab-j3-2`

## Objectif pédagogique

Durcir et industrialiser la qualité de Pulse : tests réactifs avancés, intégration sur de
vraies bases via Testcontainers, **garantie d'absence de blocage avec BlockHound**, et une
CI qui verrouille tout. À la fin, « rien ne bloque » n'est plus un espoir — le build le prouve.

## Travail à réaliser

### Partie A — StepVerifier avancé

Ajoutez des tests qui vont au-delà de l'`expectNext` :
- **TestPublisher** pour piloter la source du pipeline d'ingestion (émettre next/error
  quand on veut) et vérifier la réaction aux erreurs.
- **PublisherProbe** sur le fallback du fan-out (`/api/health/aggregate`) : prouver que
  la branche de repli a bien été souscrite quand un upstream échoue (`assertWasSubscribed`).
- Un test en **temps virtuel** sur le back-off d'enrichissement (rappel J1-2).

Lancez `mvn verify`.

### Partie B — tests web

- Slice test `@WebFluxTest(AlertController.class)` avec `WebTestClient` + **`@MockitoBean`**
  sur le service (pas `@MockBean`, déprécié).
- Un test du flux SSE `/api/metrics/stream` : `returnResult(...).getResponseBody()` →
  `StepVerifier` sur `body.take(n)`.
- Optionnel : un jeu de **tests de contrat** (mêmes entrées/sorties) exécuté contre
  `pulse-reactive` et `pulse-mvc-loom` pour verrouiller l'équivalence fonctionnelle.

### Partie C — Testcontainers

Consolidez les tests d'intégration avec **`@ServiceConnection`** (plus de
`@DynamicPropertySource`) : `PostgreSQLContainer` et `KafkaContainer`. Écrivez **le test
bout-en-bout** : publier un message Kafka → vérifier qu'il ressort sur le SSE via
`WebTestClient`, avec le **même `traceId`** corrélé (lien avec J3-1/J3-2). Pas de H2 en
remplacement de R2DBC.

### Partie D — BlockHound

Ajoutez `blockhound-junit-platform` en **scope test** (épinglez une version compatible
avec **JDK 25** — l'instrumentation y est sensible). Lancez la suite : BlockHound doit
échouer sur tout appel bloquant exécuté sur un pool non bloquant. Pour le blocage
**intentionnel et isolé** sur `boundedElastic` (l'offload JDBC simulé), ajoutez une entrée
d'**allowlist documentée** (commentaire expliquant pourquoi). Tout le reste reste interdit.

### Partie E — pipeline CI

Créez un workflow CI (GitHub Actions ou équivalent) à la racine du repo, avec ces étapes
**bloquantes pour le merge** : build, tests unitaires (StepVerifier), tests d'intégration
(Testcontainers — Docker disponible dans la CI), BlockHound. Versions gérées par le BOM
Boot (aucun pin manuel). Documentez les gates dans le README.

## Critères d'acceptation

- [ ] `mvn verify` vert, incluant TestPublisher, PublisherProbe et un test en temps virtuel.
- [ ] Slice `@WebFluxTest` avec `@MockitoBean` ; test SSE via `returnResult` + `StepVerifier`.
- [ ] Intégration via **Testcontainers + `@ServiceConnection`** (Postgres + Kafka) ; test bout-en-bout Kafka → SSE avec `traceId` corrélé.
- [ ] **BlockHound actif** ; la suite échoue sur un blocage non autorisé ; allowlist limitée et **commentée**.
- [ ] Workflow CI bloquant pour le merge : build + unit + intégration + BlockHound ; versions via BOM.
- [ ] Aucun pin de version sur les libs gérées par le BOM.

## Points à faire verbaliser

- Pourquoi PublisherProbe est-il plus fiable qu'un compteur maison pour prouver qu'une branche a été prise ?
- Qu'est-ce qu'un test H2 « à la place » de R2DBC pourrait masquer ou inventer comme bug ?
- Que prouve concrètement BlockHound que les autres tests ne prouvent pas ?
- Une allowlist BlockHound qui grossit : quel signal sur la conception ?

## Pièges à éviter

- `block()` dans un test « pour simplifier » → `StepVerifier` / `WebTestClient`.
- H2 en intégration à la place de PostgreSQL R2DBC → Testcontainers.
- `@MockBean` (déprécié) → `@MockitoBean`.
- Désactiver BlockHound ou élargir l'allowlist pour faire passer un test → corriger le blocage.
- Pin de versions Reactor/Testcontainers à la main → laisser le BOM.

## Validation & solution de référence

- `mvn verify` vert, BlockHound actif, pipeline CI ; gates documentés.
- Solution complète au tag `lab-j4-1` (`git checkout lab-j4-1`).
