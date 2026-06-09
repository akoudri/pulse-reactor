# Lab J4-2 — Sécurité, profiling & décision de migration

**Durée cible** : ~3 h · **Modules** : `pulse-reactive`, `pulse-loadtest`, `docs/` · **Solution de référence** : tag `lab-j4-2`
**Infra** : `docker compose --profile data --profile streaming --profile upstream up -d` · **Point de départ** : tag `lab-j4-1`

## Objectif pédagogique

Le capstone. On sécurise Pulse de façon non bloquante, on le profile sous charge, puis on
**tranche la question du J1 sur des chiffres** : réactif ou MVC+Loom ? Le livrable final
n'est pas du code mais une **recommandation argumentée + un runbook** — ce qu'on remettrait
réellement à une équipe.

## Travail à réaliser

### Partie A — sécurité réactive

Ajoutez Spring Security réactif à `pulse-reactive` : `@EnableWebFluxSecurity`, un
`SecurityWebFilterChain` (DSL lambda) protégeant `/api/**` (JWT resource server OU basic
pour le lab). Dans le service de création d'alerte, lisez le principal via
**`ReactiveSecurityContextHolder`** et associez l'alerte à l'utilisateur — prouvant que le
`SecurityContext` traverse le pipeline (lien J3-1). Écrivez des tests `WebTestClient` avec
utilisateur mocké (`@WithMockUser` / mutateur jwt). Lancez `mvn verify`.

### Partie B — profiling

Lancez la charge Gatling (de J2-2) contre `pulse-reactive` avec `ReactorDebugAgent.init()`
actif et un enregistrement **JFR**. **Introduisez volontairement** un goulot (ex. un appel
bloquant non isolé, ou un `onBackpressureBuffer` non borné), observez : BlockHound (J4-1)
et/ou le profil JFR doivent le révéler. Corrigez, re-mesurez, documentez l'avant/après.

### Partie C — la mesure différentielle

Avec `pulse-loadtest`, lancez la **même** montée en charge contre `pulse-reactive` (8080)
et `pulse-mvc-loom` (8081), sur l'endpoint CRUD **et** sur le fan-out. Collectez : débit,
p99/p999, threads et mémoire à charge égale, comportement à saturation. Consignez les
chiffres dans un tableau comparatif.

### Partie D — le livrable : reco + runbook

Produisez un fichier `docs/decision-migration.md` contenant :
1. **Évaluation d'un snippet legacy** (fourni ci-dessous) : analysez-le, puis recommandez —
   *migrer vers WebFlux* / *garder MVC+Loom* / *hybrider* — **justifié par la grille de
   décision et les chiffres de la Partie C**.
2. Un **runbook** : SLO/SLI (p99, taux d'erreur, dispo), budget d'erreur, timeouts par
   appel, circuit breaker/bulkhead, stratégie de déploiement (canary + feature flag), et
   les signaux à monitorer (saturation des pools, files internes, mémoire hors-tas).

Snippet legacy à évaluer :
```java
@GetMapping("/report/{id}")
public Report report(@PathVariable Long id) {
  Customer c = jdbc.loadCustomer(id);              // bloquant
  List<Order> o = restTemplate.get(ordersUrl(id)); // bloquant
  return reportBuilder.build(c, o);                // CPU léger
}
```

## Critères d'acceptation

- [ ] `pulse-reactive` sécurisé (chaîne réactive, DSL lambda) ; principal lu via `ReactiveSecurityContextHolder` dans le pipeline ; tests verts.
- [ ] Le goulot introduit en Partie B est **détecté** (BlockHound ou JFR) puis corrigé, avant/après documenté.
- [ ] Tableau comparatif `pulse-reactive` vs `pulse-mvc-loom` (débit, p99, threads, mémoire) sur CRUD et fan-out.
- [ ] `docs/decision-migration.md` : reco argumentée sur le snippet + runbook SLO/SLI complet.
- [ ] La reco s'appuie sur la **grille** et les **mesures**, pas sur une préférence.

## Points à verbaliser

- Sur le snippet legacy : deux appels bloquants + un calcul léger — que disent la grille ET les chiffres ? (souvent : MVC+Loom suffit, voire hybride si forte concurrence).
- À charge égale, combien de threads tient chaque modèle ? Où diverge la p99 ?
- Pourquoi la dégradation à saturation est-elle un critère de décision, pas seulement le débit nominal ?
- Qu'est-ce qui, dans le runbook, est spécifique au réactif (files internes, hors-tas) ?

## Pièges à éviter

- Recommander « réactif » par principe sans regarder les chiffres → la décision se mesure.
- `UserDetailsService` / accès sécurité **bloquant** sur la chaîne réactive → isoler ou rendre réactif.
- Lire `SecurityContextHolder` (ThreadLocal) au lieu de `ReactiveSecurityContextHolder`.
- Un runbook sans timeouts ni budget d'erreur → ce n'est pas un runbook.
- Élargir l'allowlist BlockHound pour masquer le goulot de la Partie B → corriger.

## Validation & solution de référence

- `mvn verify` vert ; `docs/decision-migration.md` produit (reco + runbook).
- Solution complète au tag `lab-j4-2` (`git checkout lab-j4-2`).

> 🎓 Dernier jalon de la formation. À ce stade, `pulse-reactive`, `pulse-mvc-loom` et
> `pulse-loadtest` couvrent tout le parcours, et `docs/decision-migration.md` est le
> livrable que les apprenants repartent capables de produire.
