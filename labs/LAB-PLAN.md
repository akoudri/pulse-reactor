# Pulse — plan des labs (fil rouge, 4 jours)

Un service qui grandit checkpoint par checkpoint. Chaque lab dispose d'un **tag git**
(`lab-jX-Y`) qui matérialise sa **solution de référence** ; le tag du lab précédent sert de
**point de départ**. Un apprenant en retard (ou qui veut comparer) peut donc repartir d'un
état propre à tout moment (`git checkout lab-jX-Y`).

Chaque énoncé est autonome : l'apprenant écrit le code lui-même, puis valide avec
`mvn verify` et peut se comparer à la solution de référence.

Le **jumeau `pulse-mvc-loom`** apparaît en J2 et sert de cas différentiel jusqu'au bout.

## Progression

| Tag | Demi-journée | Ce qu'on construit | Ce que ça prouve |
|---|---|---|---|
| `lab-j1-1` | J1 AM | Parent pom (BOM Boot 4, Java 21) + `pulse-common`. Premier `Flux` d'ingestion (pur) + premier `StepVerifier`. | Bootstrap, assembly vs subscription, on teste un flux sans rien lancer. |
| `lab-j1-2` | J1 PM | Pipeline d'ingestion pur : `map`/`flatMap`/`filter`, erreurs (`onErrorResume`, `retryWhen(Retry.backoff)`), test en **virtual time**. | Maîtrise des opérateurs et de la gestion d'erreurs réactive, testée sans horloge réelle. |
| `lab-j2-1` | J2 AM | Producteur rapide / consommateur lent : Schedulers (`boundedElastic`/`parallel`), `publishOn`/`subscribeOn`, **backpressure** réel (`onBackpressureBuffer/Latest`), `checkpoint()`. | On *voit* le backpressure et l'effet des schedulers, on debugge un pipeline. |
| `lab-j2-2` | J2 PM | API WebFlux (annotée + une route fonctionnelle) + WebClient fan-out vers `upstream-sim`. **Création du jumeau `pulse-mvc-loom`** (mêmes endpoints, virtual threads). Premier run `pulse-loadtest`. | Le différentiel : même contrat, WebFlux vs MVC+Loom, première mesure. |
| `lab-j3-1` | J3 AM | Persistance : R2DBC brut (une fois) puis Spring Data R2DBC. **Propagation de contexte** (trace + sécurité) de bout en bout via `contextWrite` + Micrometer context-propagation. | Données réactives + le sujet dur du `Context` traité explicitement. |
| `lab-j3-2` | J3 PM | Temps réel : SSE + WebSocket de push des métriques au navigateur, backpressure client→DB. Ingestion **Kafka via spring-kafka bridgé en `Flux`**. Résilience (natif SF7 vs Resilience4j). | Le payoff réactif (streaming continu) + intégration messaging à l'état de l'art. |
| `lab-j4-1` | J4 AM | Tests : `WebTestClient`, slice tests, **Testcontainers** (Postgres/Kafka), **BlockHound**. Pipeline CI. | Industrialisation : on prouve l'absence de blocage et on automatise. |
| `lab-j4-2` | J4 PM | Profiling (JFR + ReactorDebugAgent), sécurité réactive. **Lab décision** : charge `pulse-reactive` vs `pulse-mvc-loom`, verdict migration + runbook SLO/SLI. | La conclusion honnête : quand le réactif paie, quand Loom suffit. |

## Conventions transverses

- Chaque lab modifie **un seul module** sauf mention contraire (`lab-j2-2` touche les
  trois apps + loadtest).
- `mvn verify` vert avant de considérer le checkpoint atteint.
- Tout le code respecte les conventions du projet : API `Sinks` (pas de `Processor`),
  `boundedElastic` (pas `elastic()`), `retryWhen(Retry.backoff)` (pas `retryBackoff`),
  pas de `block()` en pipeline, pas de `reactor-kafka`.
- Les énoncés détaillés vivent dans ce dossier (`lab-j1-1.md`, etc.), un par demi-journée.
