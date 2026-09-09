# finops-platform

Plateforme de gouvernance FinOps GitOps multi-tenant sur Kubernetes.
Détection automatique de dépassement de coût par équipe, action
corrective réelle (scale-down avec escalade progressive et rollback
automatique), traçabilité complète des décisions, le tout sur un
cluster local, sans dépendance à un fournisseur cloud.

Projet de fin d'années, ENSA Marrakech, filière RSSP.

![CI](https://github.com/Honore-M12/finops-platform/actions/workflows/ci.yml/badge.svg)

## Le problème

Mesurer le coût d'un namespace Kubernetes est aujourd'hui un problème
largement résolu : OpenCost, Kubecost et consorts donnent une vision
précise de la consommation réelle, jusqu'au conteneur. Ce que ces outils
ne font pas, c'est agir. Un dashboard qui affiche un dépassement de
budget reste un constat, pas une correction, et corriger un dépassement
suppose de toucher à des ressources qui tournent, ce qui ne peut pas
dépendre uniquement d'une alerte suivie d'une intervention manuelle
disponible au bon moment.

Ce projet part de ce constat pour construire l'étage manquant : un
opérateur qui observe le coût réel de chaque équipe, décide, agit sur
le cluster, puis revient en arrière tout seul une fois la situation
normalisée, sans jamais perdre la traçabilité qu'impose une approche
GitOps.

## Comment ça marche

Git (source de vérité)
└─ ArgoCD (App-of-Apps : apps/ → un fichier/générateur par composant)
├─ CRD FinOpsPolicy (apps/crds.yaml)
├─ ClusterPolicy Kyverno (apps/kyverno-policies.yaml)
├─ Opérateur Kopf (apps/operator.yaml)
├─ Monitoring : Prometheus, Grafana, Alertmanager, OpenCost,
│ kube-state-metrics (apps/monitoring.yaml, apps/opencost.yaml)
└─ Tenants (apps/teams-applicationset.yaml, un par
manifests/team-*/)


Trois équipes tournent aujourd'hui sur la plateforme, avec des profils
de charge volontairement différents : `team-a` (API stateless, nginx,
avec une seconde charge de travail non critique pour démontrer
l'escalade), `team-b` (worker batch, busybox), `team-c` (cache, redis).

Pour chaque tenant, la boucle de gouvernance suit le même chemin :

1. Une ressource `FinOpsPolicy` déclare un `costThreshold`, une fenêtre
   d'évaluation et une liste ordonnée d'actions correctives.
2. L'opérateur interroge Prometheus (`min_over_time` sur
   `namespace_cost_total`, produit par OpenCost à partir de la
   consommation CPU/mémoire réelle) à chaque changement de policy et,
   indépendamment, toutes les 60 secondes.
3. Un dépassement n'entraîne une action qu'une fois confirmé sur
   plusieurs cycles consécutifs. Chaque action déclarée porte une
   priorité (1 correspond à la cible la moins critique, sacrifiée en
   premier) ; si le dépassement persiste, l'escalade continue vers la
   cible suivante, avec une nouvelle confirmation à chaque étape.
4. Le rollback est automatique, mais ne se base jamais sur le coût
   observé pendant qu'une action est active, celui-ci restant
   artificiellement bas tant que la capacité est réduite. Il se base
   sur une référence gelée au moment du déclenchement.
5. Chaque action est journalisée dans `status.correctiveActionTaken`,
   exposée via `/metrics` pour Grafana, et déclenche une alerte
   Alertmanager en cas de dépassement confirmé ou d'action bloquée trop
   longtemps.

Kyverno complète cette boucle en défense en profondeur, indépendamment
de `costThreshold` : génération automatique, par tenant, d'un
`ResourceQuota`, d'un `LimitRange` et d'une `NetworkPolicy` deny-all en
ingress, dès qu'un namespace porte le label `finops-managed`.

## Ce que la mise en pratique a changé

La conception initiale de la boucle de rollback se basait sur le coût
observé en direct. En conditions réelles, ça produisait une oscillation
régulière entre action corrective et rollback : le scale-down fait
mécaniquement baisser le coût mesuré, ce qui déclenche un rollback, qui
fait remonter le coût, qui redéclenche l'action. Le correctif ne
consiste pas à ajouter un état intermédiaire pour amortir ça, mais à
geler la référence de coût au moment du déclenchement et à baser le
rollback sur cette référence plutôt que sur la mesure en direct.

Un second point, plus structurel : la CRD et les ClusterPolicy Kyverno
avaient été appliquées manuellement au tout début du projet, pour
gagner du temps. Cette application manuelle n'a jamais été reprise sous
GitOps, ce qui est resté invisible jusqu'à ce qu'une modification de
schéma poussée dans Git n'ait tout simplement aucun effet sur le
cluster. Une plateforme censée gouverner le coût d'un cluster peut donc
elle-même échapper à sa propre gouvernance, si l'un de ses éléments
constitutifs reste en dehors du périmètre qu'elle est censée
surveiller. `apps/crds.yaml` et `apps/kyverno-policies.yaml` existent
pour cette raison précise.

## Structure du dépôt

| Dossier | Contenu |
|---|---|
| `crds/` | Définition de la CRD `FinOpsPolicy` |
| `operator/` | Opérateur Kopf : `handlers.py`, 49 tests pytest, manifestes `k8s/` |
| `kyverno/` | ClusterPolicy : génération ResourceQuota/LimitRange/NetworkPolicy par tenant |
| `monitoring/` | Prometheus, Grafana, Alertmanager, kube-state-metrics |
| `apps/` | Applications/ApplicationSet ArgoCD (App-of-Apps) |
| `manifests/<tenant>/` | Namespace + FinOpsPolicy + workloads de chaque équipe |
| `argocd/root-app.yaml` | Application racine, point d'entrée GitOps unique |
| `.github/workflows/ci.yml` | Lint YAML + suite pytest, à chaque push/PR |

Pour ajouter une équipe à la plateforme sans toucher à `apps/`, voir
[`docs/onboarding-tenant.md`](docs/onboarding-tenant.md).

Voir aussi [`docs/troubleshooting.md`](docs/troubleshooting.md) et
[`docs/metrics-reference.md`](docs/metrics-reference.md).

## Prérequis

- Un cluster Kubernetes (testé sur k3d v5.9.0 / K3s v1.35.5-k3s1). K3s
  embarque un contrôleur de NetworkPolicy natif via Kube-router, aucun
  CNI additionnel n'est nécessaire.
- [ArgoCD](https://argo-cd.readthedocs.io/) installé (`--server-side`,
  requis par la taille du CRD `ApplicationSet`)
- [Kyverno](https://kyverno.io/) installé, le contrôleur lui-même se
  bootstrap manuellement, ses policies sont sous GitOps
- Python 3.12+ pour faire tourner l'opérateur et ses tests en local

## Démarrage

Une seule commande suffit, à condition qu'ArgoCD soit déjà installé sur
le cluster :

```bash
kubectl apply -f argocd/root-app.yaml
```

`finops-root` découvre ensuite tout le reste depuis `apps/` : la CRD
`FinOpsPolicy` en premier (`sync-wave: "-1"`, avant tout CR qui en
dépend), puis Kyverno, l'opérateur, le monitoring et les tenants, sans
intervention manuelle supplémentaire.

## Observabilité

- **Grafana**, dashboard multi-tenant : coût par équipe face au seuil,
  état de dépassement et d'action, réplicas, utilisation des quotas.
- **Alertmanager**, trois règles : `FinOpsBudgetExceeded` (dépassement
  confirmé sur 3 minutes), `FinOpsCorrectiveActionActive`,
  `FinOpsActionStuck` (action active depuis plus de 30 minutes).
- **Auto-observabilité de l'opérateur** (`/metrics`, port 9100),
  distincte des métriques par tenant : nombre de cycles exécutés,
  dépassements détectés, latence et taux d'échec des requêtes
  Prometheus.
- **Résilience réseau** : les appels vers Prometheus sont protégés par
  un backoff exponentiel (3 tentatives, 1s/2s/4s) avant qu'un cycle
  n'abandonne proprement, plutôt que d'échouer silencieusement.

## Tests

```bash
cd operator
pip install -r requirements-dev.txt
pytest tests/ -v
```

49 tests, couvrant l'évaluation de coût, l'escalade multi-cibles, le
rollback idempotent, la référence anti-oscillation, le backoff
Prometheus et l'auto-observabilité de l'opérateur. La CI exécute cette
suite ainsi qu'un lint YAML à chaque push sur `main`, volontairement
sans build ni déploiement d'image, cohérent avec un projet qui tourne
entièrement en local, sans registre distant.

Au-delà de cette suite automatisée, l'ensemble des mécanismes de
gouvernance (escalade, anti-oscillation, rollback, alerting, isolation
réseau, cycle de vie complet d'un tenant) a été rejoué et validé sur un
cluster réel, pas seulement simulé.

## Limites connues

- L'isolation réseau générée par Kyverno couvre le trafic entrant, pas
  le trafic sortant d'un tenant.
- Les métriques Prometheus d'un tenant supprimé restent interrogeables
  jusqu'au redémarrage de l'opérateur, le handler de suppression ne les
  nettoie pas encore.
- Le périmètre des actions correctives se limite au scaling de
  Deployments.
- L'opérateur tourne en réplique unique, sans mécanisme de reprise
  testé en cas de redémarrage pendant une action active.

Un mécanisme de break-glass ArgoCD (Sync Windows, pour contourner
`selfHeal` en cas d'urgence Git indisponible) et un canal d'alerte
externe (webhook Slack/e-mail) restent hors périmètre par choix, pas
par oubli, le receveur Alertmanager par défaut expose déjà comment
brancher l'un ou l'autre.
