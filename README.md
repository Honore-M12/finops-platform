# finops-platform

Plateforme de gouvernance FinOps GitOps multi-tenant sur Kubernetes (PFA
ENSA Marrakech, filière RSSP).

Objectif : détection automatique de dépassement de coût par namespace,
application d'une action corrective réelle (scale-down, avec escalade
progressive et rollback automatique), et traçabilité complète des
décisions — le tout sans dépendance à un fournisseur cloud, sur un
cluster Kubernetes local.

## Architecture en un coup d'œil

```
Git (source de vérité)
  └─ ArgoCD (App-of-Apps : apps/ → un fichier/générateur par composant)
       ├─ CRD FinOpsPolicy (apps/crds.yaml)
       ├─ ClusterPolicy Kyverno (apps/kyverno-policies.yaml)
       ├─ Opérateur Kopf (apps/operator.yaml)
       ├─ Monitoring : Prometheus, Grafana, Alertmanager, OpenCost,
       │  kube-state-metrics (apps/monitoring.yaml, apps/opencost.yaml)
       └─ Tenants (apps/teams-applicationset.yaml, un par
          manifests/team-*/)
```

Boucle de gouvernance, pour chaque tenant :

1. Un `FinOpsPolicy` (CRD) déclare un `costThreshold`, une
   `evaluationWindow` et une liste ordonnée d'`actions` correctives.
2. L'opérateur Kopf interroge Prometheus (`min_over_time` sur
   `namespace_cost_total`, métrique produite par OpenCost + cAdvisor +
   kube-state-metrics) toutes les 60s (timer) ou à chaque changement de
   spec (réactivité GitOps).
3. Si le dépassement est confirmé sur plusieurs cycles consécutifs
   (garde-fou anti-flapping), l'opérateur scale la cible de plus basse
   priorité ; si le dépassement persiste, il escalade vers la cible
   suivante.
4. Dès que le coût réel à pleine capacité (baseline gelée au premier
   déclenchement) repasse sous le seuil, l'opérateur restaure
   automatiquement les cibles concernées (rollback).
5. Chaque action/rollback est journalisé dans `status.correctiveActionTaken`
   (audit CRD), exposé via `/metrics` pour Grafana, et déclenche des
   alertes Alertmanager en cas de dépassement confirmé ou d'action
   bloquée durablement.

Kyverno complète cette boucle en défense en profondeur : génération
automatique, par tenant, d'un `ResourceQuota` et d'un `LimitRange`
(paramétrés par annotations sur le `Namespace`), indépendants de
`costThreshold`.

## Structure du dépôt

| Dossier | Contenu |
|---|---|
| `crds/` | Définition de la CRD `FinOpsPolicy` |
| `operator/` | Opérateur Kopf : `handlers.py`, tests pytest, manifestes `k8s/` |
| `kyverno/` | ClusterPolicy : génération de `ResourceQuota`/`LimitRange` par tenant |
| `monitoring/` | Prometheus, Grafana, Alertmanager, kube-state-metrics |
| `apps/` | Applications/ApplicationSet ArgoCD (App-of-Apps) |
| `manifests/<tenant>/` | `Namespace` + `FinOpsPolicy` + workloads de chaque tenant |
| `argocd/root-app.yaml` | Application racine (point d'entrée GitOps unique) |
| `.github/workflows/ci.yml` | CI : lint YAML + suite pytest de l'opérateur |

Voir aussi [`docs/onboarding-tenant.md`](docs/onboarding-tenant.md) pour
ajouter un nouveau tenant.

## Prérequis

- Cluster Kubernetes (testé sur k3d/K3s ; K3s inclut un contrôleur
  NetworkPolicy natif via Flannel, Calico n'est pas nécessaire ici)
- [ArgoCD](https://argo-cd.readthedocs.io/) installé (`--server-side`,
  requis par la taille du CRD `ApplicationSet`)
- [Kyverno](https://kyverno.io/) installé (le contrôleur lui-même est un
  bootstrap manuel ; ses policies, elles, sont sous GitOps — voir
  `apps/kyverno-policies.yaml`)
- Python 3.12+ pour exécuter/tester l'opérateur en local
  (`operator/requirements-dev.txt`)

## Démarrage

La CRD doit toujours être appliquée manuellement une première fois (une
Application ArgoCD ne peut pas créer le CRD dont dépendent ses propres
ressources gérées) :

```bash
kubectl apply -f crds/finopspolicy-crd.yaml
kubectl apply -f argocd/root-app.yaml
```

`finops-root` découvre ensuite automatiquement tous les composants
déclarés dans `apps/` (CRD en GitOps pour les mises à jour de schéma
suivantes, Kyverno, opérateur, monitoring, tenants), sans autre
intervention manuelle.

## Observabilité et exploitation

- **Dashboard Grafana** « FinOps - Vue multi-tenant » : coût par équipe,
  coût vs seuil, état dépassement/action, replicas, utilisation quota.
- **Alertmanager** : `FinOpsBudgetExceeded` (dépassement confirmé sur
  3 minutes), `FinOpsCorrectiveActionActive`, `FinOpsActionStuck`
  (action bloquée plus de 30 minutes).
- **Métriques d'auto-observabilité de l'opérateur** (`/metrics`, port
  9100) : `finops_operator_evaluations_total`,
  `finops_operator_exceedances_detected_total`,
  `finops_operator_prometheus_query_duration_seconds`,
  `finops_operator_prometheus_query_errors_total` — distinctes des
  métriques par tenant, elles décrivent le fonctionnement interne de
  l'opérateur lui-même.
- **Résilience réseau** : les appels de l'opérateur vers Prometheus sont
  protégés par un backoff exponentiel (3 tentatives, 1s/2s/4s) avant
  d'abandonner un cycle en `PAS_ASSEZ_DE_DONNEES`.
- **Probes** : `livenessProbe`/`readinessProbe` sur `/metrics` (limite
  assumée : confirme que le processus répond, pas que la boucle de
  réconciliation Kopf elle-même est saine — voir commentaire dans
  `operator/k8s/deployment.yaml`).

## Tests

```bash
cd operator
pip install -r requirements-dev.txt
pytest tests/ -v
```

La CI (`.github/workflows/ci.yml`) exécute cette suite ainsi qu'un lint
YAML (`yamllint -c .yamllint.yml .`) à chaque push/PR sur `main`.
Volontairement limitée à ce périmètre : pas de build/push d'image ni de
déploiement réel, cohérent avec un projet 100% local sans registre
distant ni infrastructure CI/CD institutionnelle.

## Limites connues et backlog conditionnel

Non implémentés à ce jour, par choix assumé plutôt que par oubli :
mécanisme de break-glass ArgoCD (Sync Windows, pour contourner
`selfHeal=true` en cas de panne Git/CI durant une urgence), volume
persistant pour Prometheus/Grafana (historique de coûts perdu au
redémarrage — l'audit de gouvernance, lui, ne dépend pas de ce
volume : il vit dans `status.correctiveActionTaken`, persistant via
etcd), et `NetworkPolicy` egress deny-all (nécessiterait une exception
CoreDNS explicite).
