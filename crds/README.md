# CRD FinOpsPolicy

Place ici ta définition de CRD existante (semaines 1-2), par exemple :

    crds/finopspolicy-crd.yaml

Je ne l'ai pas régénérée automatiquement pour éviter de réécrire un schéma
`openAPIV3Schema` au hasard et risquer une divergence avec celle déjà
validée sur le cluster `finops-lab`. Copie simplement le fichier existant
depuis `test-kopf/` (ou l'emplacement où tu l'as créée) vers ce dossier.
