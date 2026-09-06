# Onboarding d'un nouveau tenant

Grâce au `ApplicationSet` (`apps/teams-applicationset.yaml`), ajouter un
tenant à la gouvernance FinOps ne demande **aucune modification dans
`apps/`** : il suffit d'ajouter un dossier `manifests/team-<nom>/` dans
Git. ArgoCD le découvre automatiquement au prochain cycle de sync (par
défaut, sync automatique — pas besoin de déclencher quoi que ce soit
manuellement).

## Étape 1 — Mesurer le coût nominal du profil de charge

Avant de fixer un `costThreshold`, mesurer le coût réel du workload en
régime nominal (sans aucune action corrective active), sur un intervalle
représentatif :

```promql
namespace_cost_total{namespace="team-<nom>", cluster="finops-lab"}
```

Le seuil retenu dans ce projet est systématiquement **coût nominal
mesuré × 1.5** de marge (voir les commentaires dans les
`finopspolicy.yaml` existants pour la méthode exacte). Documenter ce
calcul dans un commentaire du fichier, pas seulement la valeur brute.

## Étape 2 — Créer les manifestes du tenant

Créer `manifests/team-<nom>/` avec au minimum :

**`namespace.yaml`**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: team-<nom>
  labels:
    finops-managed: "true"   # requis : active la génération Kyverno
                              # du ResourceQuota/LimitRange pour ce tenant
  annotations:
    finops.yougos.io/quota-cpu: "<ex: 1500m>"
    finops.yougos.io/quota-memory: "<ex: 768Mi>"
```

**`finopspolicy.yaml`**

```yaml
apiVersion: finops.yougos.io/v1
kind: FinOpsPolicy
metadata:
  name: team-<nom>-policy
  namespace: team-<nom>
spec:
  costThreshold: <mesure_nominale x 1.5>
  evaluationWindow: "10m"
  consecutiveCyclesThreshold: 3
  actions:
    - target: <nom-du-deployment>
      minReplicas: <0 ou 1 selon criticité>
      priority: 1
    # Ajouter d'autres cibles avec priority croissante pour une
    # escalade progressive (voir manifests/team-a/ pour un exemple à
    # deux cibles).
```

**Un ou plusieurs `Deployment`** représentant la charge réelle du
tenant (voir `manifests/team-a/team-a-test-workload.yaml` pour un
exemple de profil « API stateless », ou `team-b`/`team-c` pour des
profils batch/cache).

## Étape 3 — Vérifier après sync

Une fois le commit poussé sur `main` :

1. `kubectl get application -n argocd` doit faire apparaître une
   nouvelle Application `finopspolicy-team-<nom>`, `Synced`/`Healthy`.
2. `kubectl get resourcequota,limitrange -n team-<nom>` doit montrer les
   objets générés automatiquement par Kyverno (peut prendre quelques
   secondes après la création du `Namespace`).
3. Le dashboard Grafana « FinOps - Vue multi-tenant » doit faire
   apparaître le nouveau tenant dans les panels labellisés par
   `namespace`/`exported_namespace` (voir le piège de collision de label
   documenté dans le code des dashboards si une métrique semble
   manquante).

## Points d'attention connus

- **Le `Deployment` cible d'une action corrective** doit exister avant
  ou en même temps que le `FinOpsPolicy` qui le référence — sinon
  l'opérateur logue une erreur (« Deployment introuvable ») sans
  bloquer le reste de la boucle.
- **Le label `finops-managed: "true"`** est ce qui déclenche la
  génération Kyverno ; l'oublier laisse le tenant sans quota, sans
  erreur visible immédiate.
- **Le conflit ArgoCD/opérateur sur `spec.replicas`** est déjà couvert
  pour les noms de `Deployment` `test-workload` et
  `test-workload-batch-secondaire` (`ignoreDifferences` dans
  `teams-applicationset.yaml`). Un nouveau tenant utilisant un autre nom
  de cible doit ajouter une entrée `managedFieldsManagers` équivalente,
  sous peine que `selfHeal=true` annule les scale-down de l'opérateur au
  cycle de sync suivant.
