"""
Opérateur FinOpsPolicy - handlers Kopf

Surveille les ressources FinOpsPolicy, interroge Prometheus pour le coût
courant du namespace concerné, et déclenche une action corrective tracée
dans le status de la ressource en cas de dépassement de seuil soutenu sur
la fenêtre d'évaluation configurée.
"""

import json
import logging
from datetime import datetime

import kopf
import requests

PROMETHEUS_URL = "http://prometheus.monitoring.svc:9090"
CLUSTER_LABEL = "finops-lab"
SIGNATURE_ANNOTATION = "finops.yougos.io/last-handled-signature"


# ---------------------------------------------------------------------------
# Fonctions métier
#
# min_cost() ne sert plus en production : Prometheus calcule déjà le
# min_over_time côté serveur (voir build_promql). Elle reste uniquement
# pour les tests unitaires locaux, sans dépendance réseau.
# ---------------------------------------------------------------------------

def min_cost(series, window_seconds, now):
    """Équivalent Python de min_over_time — USAGE TEST UNIQUEMENT."""
    values_in_window = [v for (t, v) in series if now - window_seconds <= t <= now]
    return min(values_in_window) if values_in_window else None


def evaluate_cost(min_cost_value, cost_threshold):
    if min_cost_value is None:
        return "PAS_ASSEZ_DE_DONNEES"
    if min_cost_value > cost_threshold:
        return "DEPASSEMENT"
    return "OK"


def entry_build(mc, cost_threshold, window_seconds):
    horodatage = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{horodatage} - depassement detecte: min_cost={mc} > "
        f"threshold={cost_threshold} sur fenetre={window_seconds}"
    )


def extraire_valeur(reponse_json):
    result = reponse_json.get("data", {}).get("result", [])
    if not result:
        return None
    return float(result[0]["value"][1])


def doit_traiter(spec_actuel, derniere_signature):
    """Retourne (a_traiter, signature_actuelle). Signature basée sur spec,
    jamais sur status (qui change à chaque patch du handler lui-même)."""
    signature_actuelle = json.dumps(spec_actuel, sort_keys=True)
    return signature_actuelle != derniere_signature, signature_actuelle


# ---------------------------------------------------------------------------
# Appel réseau réel à Prometheus
# ---------------------------------------------------------------------------

def build_promql(namespace: str, window: str) -> str:
    return (
        f'min_over_time(namespace_cost_total{{namespace="{namespace}", '
        f'cluster="{CLUSTER_LABEL}"}}[{window}])'
    )


def query_prometheus(promql: str, prometheus_url: str = PROMETHEUS_URL) -> dict:
    url = f"{prometheus_url}/api/v1/query"
    try:
        response = requests.get(url, params={"query": promql}, timeout=5)
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.error(f"Appel Prometheus echoue: {exc}")
        return {"data": {"result": []}}
    return response.json()


# ---------------------------------------------------------------------------
# Handlers Kopf
# ---------------------------------------------------------------------------

@kopf.on.create('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_create(spec, meta, patch, logger, **kwargs):
    signature = json.dumps(spec, sort_keys=True)
    patch.metadata.annotations[SIGNATURE_ANNOTATION] = signature
    logger.info(f"FinOpsPolicy creee dans {meta['namespace']}, signature initiale enregistree.")


@kopf.on.update('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_update(spec, meta, status, patch, logger, **kwargs):
    derniere_signature = meta.get('annotations', {}).get(SIGNATURE_ANNOTATION, "")
    a_traiter, signature_actuelle = doit_traiter(spec, derniere_signature)

    if not a_traiter:
        logger.debug("Signature inchangee (probablement notre propre patch de status) - cycle ignore.")
        return

    namespace_cible = meta['namespace']
    cost_threshold = spec['costThreshold']
    window = spec['evaluationWindow']

    promql = build_promql(namespace_cible, window)
    reponse = query_prometheus(promql)
    valeur = extraire_valeur(reponse)

    decision = evaluate_cost(valeur, cost_threshold)

    if decision == "DEPASSEMENT":
        entree = entry_build(valeur, cost_threshold, window)
        historique = status.get('correctiveActionTaken') or []
        patch.status['correctiveActionTaken'] = historique + [entree]
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = True
        logger.warning(f"DEPASSEMENT: {entree}")
    elif decision == "OK":
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = False
        logger.info(f"OK: min_cost={valeur} <= threshold={cost_threshold}")
    else:
        logger.info("PAS_ASSEZ_DE_DONNEES: aucune metrique sur la fenetre, aucune action.")

    # Toujours mettre à jour la signature en dernier, pour que le patch de
    # status déclenché par ce même appel soit ignoré au prochain passage.
    patch.metadata.annotations[SIGNATURE_ANNOTATION] = signature_actuelle


@kopf.on.delete('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_delete(meta, logger, **kwargs):
    logger.info(f"FinOpsPolicy supprimee dans {meta['namespace']}.")
