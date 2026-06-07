# Lab J1-1 — Bootstrap & premier flux testé

**Durée cible** : ~60–75 min · **Module** : `pulse-common` (+ parent pom) · **Solution de référence** : tag `lab-j1-1`
**Infra** : aucune (flux purs, rien à lancer dans Docker)

## Objectif pédagogique

Mettre en place le squelette du projet et écrire un premier `Flux` d'ingestion de
métriques **testé sans rien exécuter au sens classique** — pour ancrer dès la première
heure l'idée que *rien ne se passe tant qu'on ne souscrit pas* (assembly-time vs
subscription-time), et que `StepVerifier` souscrit pour nous et vérifie le contrat du flux.

On ne touche **ni à WebFlux, ni à une base** : pipeline Reactor pur.

## Pré-requis (à vérifier en salle)

- `java -version` → 21 (ou 25).
- `docker --version` (pas utilisé ici, mais on valide l'environnement).
- Le projet `pulse/` ouvert dans votre IDE.

## Travail à réaliser

1. Créez le parent `pom.xml` Maven (packaging `pom`, import du BOM Spring Boot 4.0.x,
   `<maven.compiler.release>21</maven.compiler.release>`, module `pulse-common` déclaré) et
   le module `pulse-common`.
2. Dans `pulse-common`, définissez un record
   `MetricSample(String agentId, String name, double value, java.time.Instant at)`.
3. Créez une classe `IngestionPipeline` exposant
   `Flux<MetricSample> normalize(Flux<MetricSample> raw)` qui :
   - filtre les échantillons dont `value` est négative (capteur invalide) ;
   - arrondit `value` à 2 décimales ;
   - préfixe `name` par `"pulse."` s'il ne l'est pas déjà.
4. N'ajoutez **aucune** dépendance web ni base : Reactor core est fourni par le BOM. Pas de
   `block()` ni de `subscribe()` dans le code de production.
5. Écrivez un test `IngestionPipelineTest` avec **StepVerifier** couvrant : un échantillon
   valide normalisé, un échantillon négatif filtré, un `name` déjà préfixé laissé tel quel,
   et la complétion du flux.
6. Lancez `mvn verify`.

## Critères d'acceptation

- [ ] `mvn verify` est vert.
- [ ] `pulse-common` ne déclare **aucune** dépendance `web`/`webflux`/`r2dbc`/`data`.
- [ ] `normalize` ne contient ni `block()` ni `subscribe()`.
- [ ] Le test utilise `StepVerifier` (`expectNext` / `verifyComplete`), pas d'assertions
      après un `block()`.
- [ ] Les versions Spring/Reactor ne sont pinnées nulle part (gérées par le BOM).

## Points à verbaliser

- Pourquoi `normalize` ne « tourne » pas tant que personne ne souscrit ? (montrer qu'aucun
  `doOnNext` ne logge avant le `StepVerifier`).
- Quelle différence entre tester ce flux et tester une `List<MetricSample>` impérative ?
- Où s'exécute le pipeline ici, et sur quel thread ? (amorce du J2 sur les Schedulers.)

## Pièges à éviter

- `collectList().block()` suivi d'assertions JUnit classiques → non, c'est StepVerifier.
- Ajouter `spring-boot-starter-webflux` « pour avoir Reactor » → non, le BOM suffit
  (dépendance `io.projectreactor:reactor-core`, et `reactor-test` en scope test).
- Pinner une version de `reactor-core` → non, BOM.

## Validation & solution de référence

- `mvn verify` doit être vert et les critères d'acceptation cochés.
- Une solution complète est disponible au tag `lab-j1-1` (`git checkout lab-j1-1`) — à
  consulter après avoir cherché, ou pour repartir d'un état propre avant le lab suivant.
