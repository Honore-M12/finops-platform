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

## État (17 août 2026)

- [x] CRD FinOpsPolicy (semaines 1-2)
- [x] Opérateur Kopf fonctionnel, branché sur Prometheus (semaine 4)
- [x] Kyverno : génération deny-all validée sur `finops-lab` (semaine 5)
- [ ] Prometheus/Grafana (semaine 5, en cours)
- [ ] OpenCost (semaine 5)
- [ ] Validation ArgoCD end-to-end sur ce repo réel

## Avant de pousser

Remplace `<ton-user>` par ton vrai nom d'utilisateur GitHub dans :
- `argocd/root-app.yaml`
- `apps/team-a.yaml`
