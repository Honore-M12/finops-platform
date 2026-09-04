"""
Opérateur FinOpsPolicy - handlers Kopf

Surveille les ressources FinOpsPolicy, interroge Prometheus pour le coût
courant du namespace concerné, et déclenche une action corrective réelle
(scale-down du Deployment cible) en cas de dépassement de seuil confirmé
sur plusieurs cycles consécutifs. L'action est automatiquement annulée
(rollback) dès que le coût repasse sous le seuil.

Deux couches de lissage temporel protègent contre les faux positifs :
  1. Intra-fenêtre : min_over_time côté Prometheus (voir build_promql)
  2. Inter-cycles : confirmation sur `consecutiveCyclesThreshold` cycles
     consécutifs avant toute action destructive (garde-fou anti-flapping)

La ré-évaluation est déclenchée par deux sources independantes, qui
partagent le meme coeur de logique (evaluate_and_act) :
  - un changement reel de spec (on_update), pour une reactivite immediate
    coherente avec la philosophie GitOps du projet
  - un timer periodique (toutes les TIMER_INTERVAL_SECONDS), decouple de
    evaluationWindow qui ne concerne que le lissage cote PromQL - c'est le
    timer qui alimente concretement le compteur consecutiveExceedances
"""

import json
import logging
from datetime import datetime

import kopf
import requests
from kubernetes import client, config
from kubernetes.client.rest import ApiException

PROMETHEUS_URL = "http://prometheus.monitoring.svc:9090"
CLUSTER_LABEL = "finops-lab"
SIGNATURE_ANNOTATION = "finops.yougos.io/last-handled-signature"
CONSECUTIVE_CYCLES_DEFAULT = 3
TIMER_INTERVAL_SECONDS = 60  # cadence de reevaluation, decouplee de evaluationWindow
TIMER_INITIAL_DELAY_SECONDS = 30  # laisse le temps a Prometheus/OpenCost d'etre prets au demarrage


# ---------------------------------------------------------------------------
# Fonctions métier - évaluation
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


def entry_action(mc, cost_threshold, window_seconds, target, min_replicas):
    """Entrée d'audit pour une action corrective déclenchée (scale-down)."""
    horodatage = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{horodatage} - action corrective: scale-down {target} a {min_replicas} replicas "
        f"(min_cost={mc} > threshold={cost_threshold} sur fenetre={window_seconds}, "
        f"confirme sur cycles consecutifs)"
    )


def entry_rollback(target, restored_replicas):
    """Entrée d'audit pour un rollback automatique (retour sous le seuil)."""
    horodatage = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{horodatage} - rollback: {target} restaure a {restored_replicas} replicas "
        f"(seuil repasse sous costThreshold)"
    )


def extraire_valeur(reponse_json):
    result = reponse_json.get("data", {}).get("result", [])
    if not result:
        return None
    return float(result[0]["value"][1])


def doit_traiter(spec_actuel, derniere_signature):
    """Retourne (a_traiter, signature_actuelle). Signature basée sur spec,
    jamais sur status (qui change à chaque patch du handler lui-même).
    dict(spec_actuel) : Kopf fournit spec comme un objet Spec (Mapping en
    lecture seule), pas un dict natif - json.dumps() refuse de le
    sérialiser directement sans cette conversion."""
    signature_actuelle = json.dumps(dict(spec_actuel), sort_keys=True)
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
# Action corrective réelle - client Kubernetes
#
# apps_client est injectable (paramètre optionnel) pour permettre le
# mock en tests unitaires (pytest, point 4.5) sans cluster réel.
# ---------------------------------------------------------------------------

def get_deployment_replicas(namespace: str, name: str, apps_client=None):
    """Lit le nombre de replicas actuel du Deployment cible.
    Retourne None si le Deployment est introuvable ou l'appel échoue."""
    apps_client = apps_client or client.AppsV1Api()
    try:
        deployment = apps_client.read_namespaced_deployment(name=name, namespace=namespace)
        return deployment.spec.replicas
    except ApiException as exc:
        logging.error(f"Lecture Deployment {namespace}/{name} echouee: {exc}")
        return None


def scale_deployment(namespace: str, name: str, replicas: int, apps_client=None) -> bool:
    """Applique un scale sur le Deployment cible. Retourne True si succès."""
    apps_client = apps_client or client.AppsV1Api()
    try:
        apps_client.patch_namespaced_deployment_scale(
            name=name,
            namespace=namespace,
            body={"spec": {"replicas": replicas}},
        )
        return True
    except ApiException as exc:
        logging.error(f"Scale de {namespace}/{name} a {replicas} replicas echoue: {exc}")
        return False


# ---------------------------------------------------------------------------
# Cœur de la logique de décision + action
#
# Fonction volontairement séparée des handlers Kopf : réutilisée à
# l'identique par on_finops_update et on_finops_timer, sans duplication
# de logique entre les deux sources de déclenchement.
# ---------------------------------------------------------------------------

def evaluate_and_act(namespace, spec, status, patch, logger):
    cost_threshold = spec['costThreshold']
    window = spec['evaluationWindow']
    target = spec.get('targetDeployment')
    min_replicas = spec.get('minReplicas')
    cycles_threshold = spec.get('consecutiveCyclesThreshold', CONSECUTIVE_CYCLES_DEFAULT)

    promql = build_promql(namespace, window)
    reponse = query_prometheus(promql)
    valeur = extraire_valeur(reponse)
    decision = evaluate_cost(valeur, cost_threshold)

    action_state = status.get('actionState', 'NORMAL')
    consecutive = status.get('consecutiveExceedances', 0)

    if decision == "DEPASSEMENT":
        consecutive += 1
        patch.status['consecutiveExceedances'] = consecutive
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = True

        if consecutive < cycles_threshold:
            # Garde-fou anti-flapping : dépassement pas encore confirmé
            logger.warning(
                f"DEPASSEMENT (cycle {consecutive}/{cycles_threshold}), "
                f"en attente de confirmation avant action."
            )
        elif action_state == 'NORMAL':
            # Seuil de confirmation atteint et aucune action déjà active
            if not target or min_replicas is None:
                logger.error(
                    "Seuil confirme mais targetDeployment/minReplicas absents "
                    "de la spec: action corrective impossible."
                )
            else:
                replicas_avant = get_deployment_replicas(namespace, target)
                if replicas_avant is None:
                    logger.error(f"Deployment {target} introuvable, action corrective annulee.")
                else:
                    succes = scale_deployment(namespace, target, min_replicas)
                    if succes:
                        entree = entry_action(valeur, cost_threshold, window, target, min_replicas)
                        historique = status.get('correctiveActionTaken') or []
                        patch.status['correctiveActionTaken'] = historique + [entree]
                        # Etat pré-action stocké pour le rollback automatique.
                        # La detection de conflit au moment du rollback (voir
                        # branche OK ci-dessous) verifie que les replicas
                        # n'ont pas ete modifies manuellement entre-temps
                        # avant de restaurer - limitation residuelle : rien
                        # ne protege contre une modification manuelle qui
                        # tomberait pile sur min_replicas par coincidence.
                        patch.status['preActionState'] = {'replicas': replicas_avant}
                        patch.status['actionState'] = 'ACTION_TAKEN'
                        logger.warning(entree)
        else:
            logger.info(f"DEPASSEMENT persistant, action deja active sur {target}.")

    elif decision == "OK":
        patch.status['consecutiveExceedances'] = 0
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = False

        if action_state == 'ACTION_TAKEN':
            pre_action = status.get('preActionState') or {}
            replicas_a_restaurer = pre_action.get('replicas')
            if target and replicas_a_restaurer is not None:
                replicas_actuels = get_deployment_replicas(namespace, target)
                if replicas_actuels is None:
                    logger.error(f"Deployment {target} introuvable, rollback impossible.")
                elif replicas_actuels != min_replicas:
                    # Detection de conflit : l'etat reel ne correspond pas a
                    # l'etat post-action attendu (min_replicas). Quelqu'un a
                    # probablement modifie les replicas manuellement pendant
                    # que l'action corrective etait active. On ne restaure
                    # PAS automatiquement par-dessus une intervention humaine
                    # non tracee - actionState reste ACTION_TAKEN pour
                    # investigation manuelle plutot que d'ecraser en silence.
                    logger.error(
                        f"Conflit detecte avant rollback: {target} a "
                        f"{replicas_actuels} replicas, attendu {min_replicas} "
                        f"(etat post-action). Modification manuelle probable "
                        f"pendant l'action corrective. Rollback automatique "
                        f"annule, actionState reste ACTION_TAKEN."
                    )
                else:
                    succes = scale_deployment(namespace, target, replicas_a_restaurer)
                    if succes:
                        entree = entry_rollback(target, replicas_a_restaurer)
                        historique = status.get('correctiveActionTaken') or []
                        patch.status['correctiveActionTaken'] = historique + [entree]
                        patch.status['actionState'] = 'NORMAL'
                        patch.status['preActionState'] = None
                        logger.info(entree)
                    else:
                        logger.error(
                            "Rollback echoue lors du scale: actionState reste "
                            "ACTION_TAKEN pour investigation manuelle."
                        )
            else:
                logger.error(
                    "Retour sous le seuil mais preActionState absent: rollback "
                    "impossible, actionState reste ACTION_TAKEN."
                )
        else:
            logger.info(f"OK: min_cost={valeur} <= threshold={cost_threshold}")

    else:
        logger.info("PAS_ASSEZ_DE_DONNEES: aucune metrique sur la fenetre, aucune action.")


# ---------------------------------------------------------------------------
# Handlers Kopf
# ---------------------------------------------------------------------------

@kopf.on.startup()
def configure(settings, logger, **kwargs):
    """Charge la configuration Kubernetes une seule fois au démarrage de
    l'opérateur (in-cluster en production, kubeconfig local en dev)."""
    try:
        config.load_incluster_config()
        logger.info("Configuration Kubernetes chargee (in-cluster).")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("Configuration Kubernetes chargee (kubeconfig local).")


@kopf.on.create('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_create(spec, meta, patch, logger, **kwargs):
    signature = json.dumps(dict(spec), sort_keys=True)
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
    evaluate_and_act(namespace_cible, spec, status, patch, logger)

    # Toujours mettre à jour la signature en dernier, pour que le patch de
    # status déclenché par ce même appel soit ignoré au prochain passage.
    patch.metadata.annotations[SIGNATURE_ANNOTATION] = signature_actuelle


@kopf.timer(
    'finops.yougos.io', 'v1', 'finopspolicies',
    interval=TIMER_INTERVAL_SECONDS,
    initial_delay=TIMER_INITIAL_DELAY_SECONDS,
)
def on_finops_timer(spec, meta, status, patch, logger, **kwargs):
    """Source periodique d'evaluation, independante de tout changement de
    spec. C'est ce timer qui alimente concretement consecutiveExceedances
    au fil du temps - sans lui, le compteur ne progresse que sur des
    changements manuels de la FinOpsPolicy (voir on_finops_update).

    Limitation documentee (YAGNI) : si ce timer et un vrai changement de
    spec se declenchent quasi simultanement, un double comptage de cycle
    est possible en theorie. Cas rare, pas de mecanisme de debounce ajoute
    pour eviter une complexite disproportionnee par rapport au risque."""
    namespace_cible = meta['namespace']
    evaluate_and_act(namespace_cible, spec, status, patch, logger)


@kopf.on.delete('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_delete(meta, logger, **kwargs):
    logger.info(f"FinOpsPolicy supprimee dans {meta['namespace']}.")
