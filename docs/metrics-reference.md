# Référence des métriques exposées par l'opérateur

L'opérateur expose ses métriques au format Prometheus sur `/metrics`
(port 9100). Cette page documente chacune d'elles, à jour avec
`operator/handlers.py`.

## Un piège à connaître avant de les utiliser

Toutes les métriques ci-dessous portent, côté code, un label nommé
`namespace`. Une fois scrapées par Prometheus via le job de découverte
automatique des pods, ce label est systématiquement réécrit avec le
namespace technique du pod qui expose la métrique (`finops-system`), et
la valeur d'origine est renommée `exported_namespace`. Toute requête
PromQL sur ces métriques doit donc filtrer ou afficher
`exported_namespace`, jamais `namespace`. Ce comportement est standard
à Prometheus (`honor_labels: false` par défaut), pas un bug de ce
projet, mais il piège facilement quiconque écrit une nouvelle requête
sans le savoir.

## Métriques de gouvernance FinOps (par tenant)

Ces métriques décrivent l'état de chaque `FinOpsPolicy`, recalculées à
chaque cycle depuis la ressource elle-même (jamais désynchronisées de
Git).

| Métrique | Type | Labels | Signification |
|---|---|---|---|
| `finops_cost_threshold` | Gauge | `namespace`, `policy` | Seuil de coût configuré (`spec.costThreshold`) |
| `finops_min_cost_in_window` | Gauge | `namespace`, `policy` | Coût minimum observé sur `evaluationWindow` |
| `finops_baseline_cost_at_trigger` | Gauge | `namespace`, `policy` | Dernier coût à pleine capacité connu, gelé au déclenchement d'une action. Reste inchangée après un rollback (pas remise à zéro), à interpréter avec `finops_action_state` |
| `finops_threshold_exceeded` | Gauge | `namespace`, `policy` | 1 si un dépassement est considéré actif, 0 sinon |
| `finops_action_state` | Gauge | `namespace`, `policy`, `target` | 1 si une action corrective est active sur cette cible, 0 sinon |

## Métriques d'auto-observabilité de l'opérateur

Distinctes des précédentes : elles décrivent le fonctionnement interne
de l'opérateur lui-même, indépendamment de tout tenant particulier.

| Métrique | Type | Labels | Signification |
|---|---|---|---|
| `finops_operator_evaluations_total` | Counter | `namespace`, `policy` | Nombre total de cycles d'évaluation exécutés (déclencheur événementiel et minuteur confondus) |
| `finops_operator_exceedances_detected_total` | Counter | `namespace`, `policy` | Nombre total de cycles ayant détecté un dépassement, avant tout garde-fou anti-flapping (compte chaque cycle, pas chaque action) |
| `finops_operator_prometheus_query_duration_seconds` | Histogram | aucun | Latence des requêtes PromQL vers Prometheus, tentatives de retry incluses |
| `finops_operator_prometheus_query_errors_total` | Counter | `outcome` | Tentatives de requête Prometheus ayant échoué. `outcome="retry"` pour un échec suivi d'une nouvelle tentative, `outcome="exhausted"` une fois les 3 tentatives épuisées |

## Exemples de requêtes utiles

Coût actuel face au seuil, tous tenants confondus :

```promql
finops_min_cost_in_window / finops_cost_threshold
```

Taux d'échec des requêtes Prometheus sur les 10 dernières minutes :

```promql
rate(finops_operator_prometheus_query_errors_total[10m])
```

Nombre de cibles actuellement sous action corrective, tous tenants :

```promql
sum(finops_action_state)
```
