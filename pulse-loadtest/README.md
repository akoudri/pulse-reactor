# pulse-loadtest — différentiel de charge Gatling

Départage `pulse-reactive` (WebFlux, 8080) et `pulse-mvc-loom` (MVC + virtual threads, 8081)
**à la charge**, sur le cas le plus discriminant : le fan-out `GET /api/health/aggregate`.

> ⚠️ **Non lancé par `mvn verify`.** Le plugin Gatling n'est lié à aucune phase : `mvn verify`
> se contente de **compiler** la simulation. Le tir réel se déclenche à la main (`gatling:test`)
> et exige que les deux apps **et** l'upstream-sim tournent. Voir « Lancer les deux runs ».

## Pré-requis (les trois doivent tourner pendant le tir)

1. **upstream-sim** (les 3 endpoints `/health` stubbés par WireMock) :
   ```bash
   docker compose --profile upstream up -d   # écoute sur :8089
   ```
2. **l'app à mesurer** (une à la fois, voir ci-dessous).

## Lancer les deux runs

Même simulation, on change seulement `-Dpulse.baseUrl`.

### Run 1 — réactif (port 8080)
```bash
# terminal A : démarrer l'app réactive
mvn -pl pulse-reactive spring-boot:run

# terminal B : tirer contre 8080
mvn -pl pulse-loadtest gatling:test \
    -Dgatling.simulationClass=fr.janus.pulse.loadtest.HealthAggregateSimulation \
    -Dpulse.baseUrl=http://localhost:8080
```

### Run 2 — jumeau MVC+Loom (port 8081)
```bash
# terminal A : démarrer le jumeau
mvn -pl pulse-mvc-loom spring-boot:run

# terminal B : tirer contre 8081
mvn -pl pulse-loadtest gatling:test \
    -Dgatling.simulationClass=fr.janus.pulse.loadtest.HealthAggregateSimulation \
    -Dpulse.baseUrl=http://localhost:8081
```

## Où lire la comparaison

Chaque tir produit un rapport HTML horodaté :
```
pulse-loadtest/target/gatling/healthaggregatesimulation-<timestamp>/index.html
```
Comparer entre les deux runs :

| Métrique | Où | Ce qu'on regarde |
|---|---|---|
| **Débit** (req/s tenu) | « Requests / sec » | qui encaisse le plus de charge sans s'effondrer |
| **p99** (latence sous charge) | tableau « Response Time Percentiles », colonne 99th | la queue de distribution, pas la moyenne |
| **% d'échecs / KO** | « Number of requests » (OK vs KO) | dégradation sous l'upstream lent/instable |

Le point honnête à verbaliser : sous l'upstream lent (~700 ms), on s'attend à ce que le réactif
ne consomme pas un thread par requête en attente ; le jumeau tient grâce aux virtual threads.
Sur ce CRUD I/O-bound, l'écart peut être faible — c'est précisément le résultat à mesurer, pas
à présupposer. **La latence d'une requête isolée n'est pas le sujet** : on compare débit et p99
sous charge soutenue.

## Notes

- Versions Gatling pinnées dans le `pom.xml` (`gatling-maven-plugin` 4.x + `gatling-charts-highcharts`
  3.13.x). À reconfirmer/bumper via context7/web avant un vrai tir.
- Réglage fin de la charge (paliers, durées, seuils d'assertion) : peut déborder en J4.
