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
import time
from datetime import datetime

import kopf
import requests
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prometheus_client import start_http_server, Counter, Gauge, Histogram


PROMETHEUS_URL = "http://prometheus.monitoring.svc:9090"
CLUSTER_LABEL = "finops-lab"
SIGNATURE_ANNOTATION = "finops.yougos.io/last-handled-signature"
CONSECUTIVE_CYCLES_DEFAULT = 3
TIMER_INTERVAL_SECONDS = 60
TIMER_INITIAL_DELAY_SECONDS = 30
METRICS_PORT = 9100

# Résilience réseau vers Prometheus (6.2). Backoff exponentiel simple
# (1s, 2s, 4s...) plutôt qu'une dépendance externe (tenacity) : le besoin
# est un nombre fixe de tentatives avec délai croissant, pas de politique
# de retry sophistiquée (jitter, circuit breaker) - complexité non
# justifiée pour un seul appel HTTP interne au cluster.
PROM_QUERY_TIMEOUT_SECONDS = 5
PROM_QUERY_MAX_ATTEMPTS = 3
PROM_QUERY_BACKOFF_BASE_SECONDS = 1

# Metriques exposees pour le dashboard Grafana (5.6). Source de verite
# unique lue depuis le CR a chaque cycle - jamais desynchronisee de Git,
# contrairement a des seuils codes en dur dans un dashboard.
GAUGE_COST_THRESHOLD = Gauge(
    'finops_cost_threshold', 'Seuil de cout configure (spec.costThreshold)',
    ['namespace', 'policy'],
)
GAUGE_MIN_COST_IN_WINDOW = Gauge(
    'finops_min_cost_in_window', 'Cout minimum observe sur evaluationWindow (min_over_time)',
    ['namespace', 'policy'],
)
GAUGE_BASELINE_COST = Gauge(
    'finops_baseline_cost_at_trigger',
    'Dernier cout a pleine capacite connu (gele au declenchement) - '
    'valeur laissee telle quelle (non remise a zero) apres un rollback, '
    'a interpreter conjointement avec finops_action_state',
    ['namespace', 'policy'],
)
GAUGE_THRESHOLD_EXCEEDED = Gauge(
    'finops_threshold_exceeded',
    '1 si un depassement est considere actif (voir logique de baseline), 0 sinon',
    ['namespace', 'policy'],
)
GAUGE_ACTION_STATE = Gauge(
    'finops_action_state',
    '1 si une action corrective est active sur cette cible (ACTION_TAKEN), 0 sinon (NORMAL)',
    ['namespace', 'policy', 'target'],
)

# Auto-observabilité de l'opérateur lui-même (6.1) : distincte des
# métriques ci-dessus, qui décrivent l'état FinOps de chaque tenant. Ici,
# on décrit le fonctionnement interne de l'opérateur - cohérence
# architecturale avec le reste de la plateforme (tout ce qui tourne dans
# le cluster est observable via Prometheus, pas seulement les tenants).
COUNTER_EVALUATIONS_TOTAL = Counter(
    'finops_operator_evaluations_total',
    "Nombre total de cycles d'evaluation executes (on_update + timer confondus)",
    ['namespace', 'policy'],
)
COUNTER_EXCEEDANCES_TOTAL = Counter(
    'finops_operator_exceedances_detected_total',
    'Nombre total de cycles ayant detecte un DEPASSEMENT (avant tout '
    'garde-fou anti-flapping - compte chaque cycle, pas chaque action)',
    ['namespace', 'policy'],
)
HISTOGRAM_PROMETHEUS_QUERY_DURATION = Histogram(
    'finops_operator_prometheus_query_duration_seconds',
    "Latence des requetes PromQL vers Prometheus, tentatives de retry incluses",
)
COUNTER_PROMETHEUS_QUERY_ERRORS_TOTAL = Counter(
    'finops_operator_prometheus_query_errors_total',
    'Nombre total de tentatives de requete Prometheus ayant echoue (avant '
    'ou apres epuisement des retries, voir le label "outcome")',
    ['outcome'],
)


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


def query_prometheus(
    promql: str,
    prometheus_url: str = PROMETHEUS_URL,
    max_attempts: int = PROM_QUERY_MAX_ATTEMPTS,
    backoff_base_seconds: float = PROM_QUERY_BACKOFF_BASE_SECONDS,
) -> dict:
    """Interroge Prometheus avec backoff exponentiel (6.2).

    Une indisponibilite momentanee de Prometheus (redemarrage de pod,
    pic de charge) ne doit pas se traduire immediatement par un cycle
    PAS_ASSEZ_DE_DONNEES : jusqu'a `max_attempts` tentatives, avec un
    delai croissant (backoff_base_seconds, 2x, 4x...) entre chacune.
    Abandon silencieux (resultat vide) seulement apres epuisement des
    tentatives - le comportement en aval (evaluate_cost) reste inchange,
    aucune nouvelle branche de decision introduite."""
    url = f"{prometheus_url}/api/v1/query"
    start = time.monotonic()
    try:
        for tentative in range(1, max_attempts + 1):
            try:
                response = requests.get(
                    url, params={"query": promql}, timeout=PROM_QUERY_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                COUNTER_PROMETHEUS_QUERY_ERRORS_TOTAL.labels(
                    outcome="retry" if tentative < max_attempts else "exhausted"
                ).inc()
                if tentative == max_attempts:
                    logging.error(
                        f"Appel Prometheus echoue apres {max_attempts} tentatives: {exc}"
                    )
                    return {"data": {"result": []}}
                delai = backoff_base_seconds * (2 ** (tentative - 1))
                logging.warning(
                    f"Appel Prometheus echoue (tentative {tentative}/{max_attempts}): "
                    f"{exc} - nouvelle tentative dans {delai}s."
                )
                time.sleep(delai)
    finally:
        HISTOGRAM_PROMETHEUS_QUERY_DURATION.observe(time.monotonic() - start)


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

def evaluate_and_act(namespace, policy_name, spec, status, patch, logger):
    cost_threshold = spec['costThreshold']
    window = spec['evaluationWindow']
    actions_spec = sorted(spec.get('actions', []), key=lambda a: a['priority'])
    cycles_threshold = spec.get('consecutiveCyclesThreshold', CONSECUTIVE_CYCLES_DEFAULT)

    COUNTER_EVALUATIONS_TOTAL.labels(namespace=namespace, policy=policy_name).inc()

    promql = build_promql(namespace, window)
    reponse = query_prometheus(promql)
    valeur = extraire_valeur(reponse)
    decision = evaluate_cost(valeur, cost_threshold)

    if decision == "DEPASSEMENT":
        COUNTER_EXCEEDANCES_TOTAL.labels(namespace=namespace, policy=policy_name).inc()

    consecutive = status.get('consecutiveExceedances', 0)
    last_action_at_cycle = status.get('lastActionAtCycle', 0)
    actions_state = dict(status.get('actionsState') or {})
    baseline = status.get('baselineCostAtTrigger')

    def etat_de(target):
        return actions_state.get(target, {'actionState': 'NORMAL', 'preActionState': None})

    une_action_active = any(v['actionState'] == 'ACTION_TAKEN' for v in actions_state.values())

    # minCostInWindow rapporte fidelement la valeur mesuree (min_over_time
    # sur evaluationWindow), sans interpretation - le nom precedent
    # "currentCost" etait trompeur, ce n'est jamais le cout instantane.
    patch.status['minCostInWindow'] = valeur

    if decision == "DEPASSEMENT":
        consecutive += 1
        patch.status['consecutiveExceedances'] = consecutive
        patch.status['thresholdExceeded'] = True

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

                        # Baseline geleee UNIQUEMENT au premier declenchement
                        # d'une "campagne" (aucune cible encore active avant
                        # cette action) : c'est la derniere mesure fiable du
                        # cout a pleine capacite, avant toute reduction. Les
                        # escalades suivantes ne l'ecrasent pas.
                        if not une_action_active:
                            patch.status['baselineCostAtTrigger'] = valeur

    elif decision == "OK":
        if not une_action_active:
            # Rien n'est actif : etat normal confirme, pas de baseline a
            # gerer.
            patch.status['consecutiveExceedances'] = 0
            patch.status['thresholdExceeded'] = False
            patch.status['lastActionAtCycle'] = 0
            logger.info(f"OK: min_cost={valeur} <= threshold={cost_threshold}")

        elif baseline is None or baseline <= cost_threshold:
            # baseline absente (cas limite : action prise avant ce
            # mecanisme, ou etat legacy) -> pas de raison connue de
            # bloquer le rollback, on procede normalement. Sinon,
            # baseline connue et repassee sous le seuil courant :
            # rollback legitime (voir commentaire ci-dessus).
            patch.status['consecutiveExceedances'] = 0
            patch.status['thresholdExceeded'] = False
            patch.status['lastActionAtCycle'] = 0
            patch.status['baselineCostAtTrigger'] = None

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
                    logger.error(
                        f"Conflit detecte avant rollback de {target}: {replicas_actuels} "
                        f"replicas, ni l'etat post-action ({min_replicas}) ni l'etat "
                        f"pre-action ({replicas_a_restaurer}). Rollback annule pour cette cible."
                    )

            if etats_modifies:
                patch.status['correctiveActionTaken'] = historique_courant
                patch.status['actionsState'] = actions_state

        else:
            # Cout observe bas, mais c'est un artefact de l'action
            # corrective active : le cout a pleine capacite (baseline
            # gelee) reste au-dessus du seuil. On NE rollback PAS - sans
            # ca, le systeme oscillerait indefiniment entre action et
            # rollback premature (voir limitation decouverte le 2026-09-05
            # sur team-a).
            patch.status['thresholdExceeded'] = True
            logger.info(
                f"Cout observe ({valeur}) sous le seuil, mais cout a pleine capacite "
                f"({baseline}) toujours au-dessus de {cost_threshold} - action(s) "
                f"maintenue(s) active(s), pas de rollback."
            )

    else:
        logger.info("PAS_ASSEZ_DE_DONNEES: aucune metrique sur la fenetre, aucune action.")

    # Mise a jour des metriques Prometheus (dashboard Grafana, 5.6). Lit
    # l'etat final via patch.status quand ce cycle l'a modifie, sinon
    # retombe sur l'etat precedent (status/variables locales deja
    # capturees en debut de fonction) - reflete fidelement la realite
    # meme sur les cycles qui n'ont rien change.
    final_threshold_exceeded = patch.status.get('thresholdExceeded', status.get('thresholdExceeded', False))
    final_baseline = patch.status.get('baselineCostAtTrigger', baseline)
    final_actions_state = patch.status.get('actionsState', actions_state)

    GAUGE_COST_THRESHOLD.labels(namespace=namespace, policy=policy_name).set(cost_threshold)
    GAUGE_THRESHOLD_EXCEEDED.labels(namespace=namespace, policy=policy_name).set(
        1 if final_threshold_exceeded else 0
    )
    if valeur is not None:
        GAUGE_MIN_COST_IN_WINDOW.labels(namespace=namespace, policy=policy_name).set(valeur)
    if final_baseline is not None:
        GAUGE_BASELINE_COST.labels(namespace=namespace, policy=policy_name).set(final_baseline)
    for a in actions_spec:
        target = a['target']
        etat_cible = final_actions_state.get(target, {'actionState': 'NORMAL'})
        GAUGE_ACTION_STATE.labels(namespace=namespace, policy=policy_name, target=target).set(
            1 if etat_cible['actionState'] == 'ACTION_TAKEN' else 0
        )


# ---------------------------------------------------------------------------
# Handlers Kopf
# ---------------------------------------------------------------------------

@kopf.on.startup()
def configure(settings, logger, **kwargs):
    """Charge la configuration Kubernetes une seule fois au démarrage de
    l'opérateur (in-cluster en production, kubeconfig local en dev), et
    démarre le serveur /metrics pour Prometheus (dashboard Grafana, 5.6)."""
    try:
        config.load_incluster_config()
        logger.info("Configuration Kubernetes chargee (in-cluster).")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("Configuration Kubernetes chargee (kubeconfig local).")

    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Serveur /metrics demarre sur le port {METRICS_PORT}.")
    except OSError as exc:
        logger.error(f"Impossible de demarrer le serveur /metrics: {exc}")


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
    evaluate_and_act(namespace_cible, meta['name'], spec, status, patch, logger)

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
    evaluate_and_act(namespace_cible, meta['name'], spec, status, patch, logger)


@kopf.on.delete('finops.yougos.io', 'v1', 'finopspolicies')
def on_finops_delete(meta, logger, **kwargs):
    logger.info(f"FinOpsPolicy supprimee dans {meta['namespace']}.")
