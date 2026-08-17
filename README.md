# finops-platform

Plateforme de gouvernance FinOps GitOps multi-tenant sur Kubernetes (PFA).

## Structure

- `crds/` — définition de la CRD `FinOpsPolicy` (à copier depuis ton travail existant)
- `operator/` — opérateur Kopf (`handlers.py`), logique de décision + appel Prometheus
- `kyverno/` — policy de génération automatique de NetworkPolicy deny-all par tenant
- `monitoring/` — Prometheus, Grafana, OpenCost (à remplir, semaine 5)
- `apps/` — Applications ArgoCD enfants (une par tenant), découvertes par `argocd/root-app.yaml`
- `manifests/<tenant>/` — Namespace + CR FinOpsPolicy de chaque tenant
- `argocd/root-app.yaml` — Application racine (pattern App-of-Apps)

## Prérequis

- Cluster Kubernetes (testé sur k3d/K3s)
- [ArgoCD](https://argo-cd.readthedocs.io/) installé sur le cluster
- [Kyverno](https://kyverno.io/) installé sur le cluster
- Python 3.12+ pour exécuter l'opérateur (`operator/requirements.txt`)

## Démarrage

```bash
kubectl apply -f crds/finopspolicy-crd.yaml
kubectl apply -f kyverno/generate-deny-all-networkpolicy.yaml
kubectl apply -f argocd/root-app.yaml
```

L'Application racine ArgoCD (pattern App-of-Apps) découvre ensuite
automatiquement les Applications enfants déclarées dans `apps/`, une par
tenant, et synchronise leurs manifestes depuis `manifests/<tenant>/`.
