# Diagnostic rapide

Aide-mémoire des symptômes déjà rencontrés sur ce projet et de leur
cause. À consulter avant de repartir de zéro sur une investigation.

## Un panel Grafana reste vide alors que la métrique existe

Vérifier que la requête utilise `exported_namespace` et pas `namespace`.
Voir `docs/metrics-reference.md` pour le détail de ce piège de label.

## Une modification poussée dans Git n'a aucun effet sur le cluster

Vérifier que la ressource concernée est bien gérée par une Application
ArgoCD, pas appliquée manuellement à un moment donné du projet :

```bash
kubectl get application -n argocd
```

Si la ressource attendue n'apparaît dans aucune Application, c'est
probablement le même trou de gouvernance que celui rencontré avec la CRD
et les ClusterPolicy Kyverno en tout début de projet (voir
`apps/crds.yaml` et `apps/kyverno-policies.yaml`).

## Le nombre de réplicas d'un Deployment revient à sa valeur d'origine peu après un scale-down

ArgoCD, en `selfHeal` actif, restaure toute divergence sur `spec.replicas`
qu'il ne reconnaît pas comme légitime. Vérifier que le nom du Deployment
cible est bien couvert par une entrée `managedFieldsManagers` dans
`apps/teams-applicationset.yaml`. Un nouveau nom de cible, différent de
`test-workload` ou `test-workload-batch-secondaire`, doit ajouter sa
propre entrée.

## Une policy semble alterner entre action corrective et rollback en boucle

Vérifier `finops_baseline_cost_at_trigger` face à `costThreshold` : si le
seuil est durablement inférieur au coût réel de repos du tenant (charge
de travail à pleine capacité), l'oscillation redémarre à chaque
recalibrage insuffisant. Le seuil doit toujours être fixé au-dessus du
coût nominal mesuré, avec la marge décrite dans
`docs/onboarding-tenant.md`.

## L'opérateur logue « Deployment introuvable » pour une cible

Le `Deployment` référencé dans `spec.actions[].target` n'existe pas
encore dans le namespace au moment de l'évaluation. L'opérateur logue
l'erreur pour cette cible et continue son cycle normalement pour les
autres, il ne bloque pas la boucle entière.

## Un tenant n'a ni ResourceQuota ni LimitRange ni NetworkPolicy

Vérifier que le `Namespace` du tenant porte bien le label
`finops-managed: "true"`, seul déclencheur des trois ClusterPolicy
Kyverno correspondantes. L'absence de ce label ne produit aucune erreur
visible, juste une absence silencieuse de ces trois ressources.

## Un appel Prometheus échoue de façon intermittente

L'opérateur retente automatiquement jusqu'à 3 fois avec un délai
croissant (1s, 2s, 4s) avant d'abandonner le cycle en
`PAS_ASSEZ_DE_DONNEES`. Vérifier
`finops_operator_prometheus_query_errors_total` par valeur de
`outcome` pour distinguer un échec ponctuel absorbé par le retry d'un
échec persistant qui épuise les tentatives.
