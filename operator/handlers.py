"""
Opérateur FinOpsPolicy - handlers Kopf

Surveille les ressources FinOpsPolicy, interroge Prometheus pour le coût
courant du namespace concerné, et déclenche une escalade progressive
d'actions correctives réelles (scale-down de Deployments cibles, par
ordre de priorité croissante) en cas de dépassement de seuil confirmé
sur plusieurs cycles consécutifs. Chaque action est automatiquement
annulée (rollback) dès que le coût repasse sous le seuil.

Escalade multi-cibles : une FinOpsPolicy peut déclarer plusieurs cibles
(spec.actions), chacune avec une priorité. La cible de priorité la plus
basse est scalée en premier ; les suivantes ne sont déclenchées que si
le dépassement persiste malgré l'action précédente (nouvelle
confirmation sur consecutiveCyclesThreshold cycles avant chaque
escalade, pas seulement au premier déclenchement).

Deux couches de lissage temporel protègent contre les faux positifs :
  1. Intra-fenêtre : min_over_time côté Prometheus (voir build_promql)
  2. Inter-cycles : confirmation sur `consecutiveCyclesThreshold` cycles
     consécutifs avant toute action destructive OU escalade (garde-fou
     anti-flapping)

La ré-évaluation est déclenchée par deux sources indépendantes, qui
partagent le même cœur de logique (evaluate_and_act) :
  - un changement réel de spec (on_update), pour une réactivité immédiate
    cohérente avec la philosophie GitOps du projet
  - un timer périodique (toutes les TIMER_INTERVAL_SECONDS), découplé de
    evaluationWindow qui ne concerne que le lissage côté PromQL - c'est le
    timer qui alimente concrètement le compteur consecutiveExceedances
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
TIMER_INTERVAL_SECONDS = 60
TIMER_INITIAL_DELAY_SECONDS = 30


# ---------------------------------------------------------------------------
# Fonctions métier - évaluation
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


def entry_action(mc, cost_threshold, window_seconds, target, min_replicas, priority, total_actions):
    """Entrée d'audit pour une action corrective déclenchée (scale-down),
    avec sa position dans l'ordre d'escalade de la policy."""
    horodatage = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{horodatage} - action corrective: scale-down {target} a {min_replicas} replicas "
        f"(priorite {priority}, cible {target} - {total_actions} action(s) au total dans la policy) "
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
# apps_client est injectable (paramètre optionnel) pour permettre le mock
# en tests unitaires (pytest) sans cluster réel.
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
    """Applique un scale sur le Deployment cible. Retourne True si succès.

    field_manager="finops-operator" explicite : permet a ArgoCD de
    distinguer nos changements (via managedFieldsManagers dans
    ignoreDifferences) de ceux issus de Git, sans bloquer Git pour les
    equipes/cibles qui n'ont pas d'action corrective active."""
    apps_client = apps_client or client.AppsV1Api()
    try:
        apps_client.patch_namespaced_deployment_scale(
            name=name,
            namespace=namespace,
            body={"spec": {"replicas": replicas}},
            field_manager="finops-operator",
        )
        return True
    except ApiException as exc:
        logging.error(f"Scale de {namespace}/{name} a {replicas} replicas echoue: {exc}")
        return False


# ---------------------------------------------------------------------------
# Cœur de la logique de décision + action, avec escalade multi-cibles
#
# Fonction volontairement séparée des handlers Kopf : réutilisée à
# l'identique par on_finops_update et on_finops_timer.
# ---------------------------------------------------------------------------

def evaluate_and_act(namespace, spec, status, patch, logger):
    cost_threshold = spec['costThreshold']
    window = spec['evaluationWindow']
    actions_spec = sorted(spec.get('actions', []), key=lambda a: a['priority'])
    cycles_threshold = spec.get('consecutiveCyclesThreshold', CONSECUTIVE_CYCLES_DEFAULT)

    promql = build_promql(namespace, window)
    reponse = query_prometheus(promql)
    valeur = extraire_valeur(reponse)
    decision = evaluate_cost(valeur, cost_threshold)

    consecutive = status.get('consecutiveExceedances', 0)
    last_action_at_cycle = status.get('lastActionAtCycle', 0)
    actions_state = dict(status.get('actionsState') or {})

    def etat_de(target):
        return actions_state.get(target, {'actionState': 'NORMAL', 'preActionState': None})

    if decision == "DEPASSEMENT":
        consecutive += 1
        patch.status['consecutiveExceedances'] = consecutive
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = True

        # Meme condition sert au premier declenchement ET a chaque
        # escalade suivante : il faut re-confirmer sur cycles_threshold
        # cycles depuis la DERNIERE action, pas seulement depuis le debut
        # du depassement. last_action_at_cycle=0 au depart -> equivalent
        # a "depuis le debut" pour la toute premiere action.
        if consecutive - last_action_at_cycle < cycles_threshold:
            logger.warning(
                f"DEPASSEMENT (cycle {consecutive - last_action_at_cycle}/{cycles_threshold} "
                f"depuis la derniere action), en attente de confirmation."
            )
        elif not actions_spec:
            logger.error(
                "Seuil confirme mais aucune action (spec.actions) configuree: "
                "action corrective impossible."
            )
        else:
            prochaine = next(
                (a for a in actions_spec if etat_de(a['target'])['actionState'] == 'NORMAL'),
                None,
            )

            if prochaine is None:
                logger.info(
                    f"DEPASSEMENT persistant, les {len(actions_spec)} action(s) "
                    f"de la policy sont deja toutes actives."
                )
            else:
                target = prochaine['target']
                min_replicas = prochaine['minReplicas']
                priorite = prochaine['priority']

                replicas_avant = get_deployment_replicas(namespace, target)
                if replicas_avant is None:
                    logger.error(f"Deployment {target} introuvable, action annulee pour cette cible.")
                else:
                    succes = scale_deployment(namespace, target, min_replicas)
                    if succes:
                        entree = entry_action(
                            valeur, cost_threshold, window, target, min_replicas,
                            priorite, len(actions_spec),
                        )
                        historique = status.get('correctiveActionTaken') or []
                        patch.status['correctiveActionTaken'] = historique + [entree]

                        actions_state[target] = {
                            'actionState': 'ACTION_TAKEN',
                            'preActionState': {'replicas': replicas_avant},
                        }
                        patch.status['actionsState'] = actions_state
                        patch.status['lastActionAtCycle'] = consecutive
                        logger.warning(entree)

    elif decision == "OK":
        patch.status['consecutiveExceedances'] = 0
        patch.status['currentCost'] = valeur
        patch.status['thresholdExceeded'] = False
        patch.status['lastActionAtCycle'] = 0

        historique_courant = list(status.get('correctiveActionTaken') or [])
        etats_modifies = False

        for a in actions_spec:
            target = a['target']
            min_replicas = a['minReplicas']
            etat = etat_de(target)

            if etat['actionState'] != 'ACTION_TAKEN':
                continue

            pre_action = etat.get('preActionState') or {}
            replicas_a_restaurer = pre_action.get('replicas')

            if replicas_a_restaurer is None:
                logger.error(
                    f"Retour sous le seuil mais preActionState absent pour {target}: "
                    f"rollback impossible, actionState reste ACTION_TAKEN."
                )
                continue

            replicas_actuels = get_deployment_replicas(namespace, target)

            if replicas_actuels is None:
                logger.error(f"Deployment {target} introuvable, rollback impossible.")

            elif replicas_actuels == replicas_a_restaurer:
                # Rollback deja effectif (ex. ArgoCD a deja restaure Git
                # avant l'operateur). Idempotence : pas de scale inutile.
                entree = entry_rollback(target, replicas_a_restaurer)
                historique_courant.append(entree)
                actions_state[target] = {'actionState': 'NORMAL', 'preActionState': None}
                etats_modifies = True
                logger.info(f"Rollback deja effectif: {target} est deja a {replicas_a_restaurer} replicas.")

            elif replicas_actuels == min_replicas:
                succes = scale_deployment(namespace, target, replicas_a_restaurer)
                if succes:
                    entree = entry_rollback(target, replicas_a_restaurer)
                    historique_courant.append(entree)
                    actions_state[target] = {'actionState': 'NORMAL', 'preActionState': None}
                    etats_modifies = True
                    logger.info(entree)
                else:
                    logger.error(
                        f"Rollback de {target} echoue lors du scale: actionState "
                        f"reste ACTION_TAKEN pour investigation manuelle."
                    )
            else:
                # Etat inattendu : intervention externe probable. On ne
                # touche pas au Deployment pour ne pas ecraser une
                # modification potentiellement volontaire.
                logger.error(
                    f"Conflit detecte avant rollback de {target}: {replicas_actuels} "
                    f"replicas, ni l'etat post-action ({min_replicas}) ni l'etat "
                    f"pre-action ({replicas_a_restaurer}). Rollback annule pour cette cible."
                )

        if etats_modifies:
            patch.status['correctiveActionTaken'] = historique_courant
            patch.status['actionsState'] = actions_state
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
    au fil du temps.

    Limitation documentee (YAGNI) : si ce timer et un vrai changement de
    spec se declenchent quasi simultanement, un double comptage de cycle
    est possible en theorie. Cas rare, pas de mecanisme de debounce ajoute
    pour eviter une complexite disproportionnee par rapport au risque."""
    namespace_cible = meta['namespace']
    evaluate_and_act(namespace_cible, spec, status, patch, logger)


@kopf.on.delete('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_delete(meta, logger, **kwargs):
    logger.info(f"FinOpsPolicy supprimee dans {meta['namespace']}.")
