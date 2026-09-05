"""
Tests pytest pour l'opérateur FinOpsPolicy (modèle multi-cibles).

Organisation :
  - Fonctions pures (evaluate_cost, doit_traiter, entry_*, extraire_valeur,
    build_promql, min_cost) : tests directs, sans mock.
  - evaluate_and_act (coeur de la logique) : tests par scenario, avec
    query_prometheus / get_deployment_replicas / scale_deployment mockes
    via monkeypatch - aucun appel reseau ni cluster reel necessaire.
    Couvre : garde-fou anti-flapping, declenchement, ESCALADE vers la
    cible suivante, rollback idempotent, vrai conflit, rollback
    simultane de plusieurs cibles.
  - get_deployment_replicas / scale_deployment : tests unitaires avec un
    apps_client factice (unittest.mock), pour la gestion d'erreur
    ApiException independamment du reste.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from kubernetes.client.rest import ApiException

import handlers


# ---------------------------------------------------------------------------
# Aides de test
# ---------------------------------------------------------------------------

def make_patch():
    """Simule l'objet `patch` fourni par Kopf."""
    return SimpleNamespace(status={}, metadata=SimpleNamespace(annotations={}))


def base_spec(**overrides):
    spec = {
        "costThreshold": 10,
        "evaluationWindow": "10m",
        "consecutiveCyclesThreshold": 3,
        "actions": [
            {"target": "test-workload", "minReplicas": 1, "priority": 1},
        ],
    }
    spec.update(overrides)
    return spec


def two_target_spec(**overrides):
    """Spec avec 2 cibles ordonnees, pour les tests d'escalade."""
    return base_spec(
        actions=[
            {"target": "batch-secondaire", "minReplicas": 0, "priority": 1},
            {"target": "test-workload", "minReplicas": 1, "priority": 2},
        ],
        **overrides,
    )


def run(spec, status, monkeypatch, cost_value=15.0, deployment_replicas=None, scale_result=True):
    """Execute evaluate_and_act avec les dependances externes mockees."""
    monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
    monkeypatch.setattr(handlers, "extraire_valeur", lambda r: cost_value)

    if deployment_replicas is not None:
        monkeypatch.setattr(handlers, "get_deployment_replicas", lambda ns, t: deployment_replicas.get(t))

    scale_mock = Mock(return_value=scale_result)
    monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

    patch = make_patch()
    handlers.evaluate_and_act("team-a", spec, status, patch, handlers.logging.getLogger("test"))
    return patch, scale_mock


# ---------------------------------------------------------------------------
# Fonctions pures - evaluation
# ---------------------------------------------------------------------------

class TestEvaluateCost:
    def test_pas_assez_de_donnees_si_aucune_valeur(self):
        assert handlers.evaluate_cost(None, 10) == "PAS_ASSEZ_DE_DONNEES"

    def test_depassement_si_strictement_superieur_au_seuil(self):
        assert handlers.evaluate_cost(15, 10) == "DEPASSEMENT"

    def test_ok_si_egal_au_seuil(self):
        assert handlers.evaluate_cost(10, 10) == "OK"

    def test_ok_si_inferieur_au_seuil(self):
        assert handlers.evaluate_cost(5, 10) == "OK"


class TestMinCost:
    def test_retourne_le_minimum_dans_la_fenetre(self):
        series = [(0, 5.0), (5, 2.0), (10, 8.0)]
        assert handlers.min_cost(series, window_seconds=20, now=10) == 2.0

    def test_ignore_les_valeurs_hors_fenetre(self):
        series = [(0, 1.0), (100, 9.0)]
        assert handlers.min_cost(series, window_seconds=10, now=100) == 9.0

    def test_retourne_none_si_aucune_valeur_dans_la_fenetre(self):
        series = [(0, 1.0)]
        assert handlers.min_cost(series, window_seconds=5, now=100) is None


class TestExtraireValeur:
    def test_extrait_la_valeur_prometheus(self):
        reponse = {"data": {"result": [{"value": [1234567890, "0.0034259125"]}]}}
        assert handlers.extraire_valeur(reponse) == pytest.approx(0.0034259125)

    def test_retourne_none_si_resultat_vide(self):
        assert handlers.extraire_valeur({"data": {"result": []}}) is None

    def test_retourne_none_si_champ_data_absent(self):
        assert handlers.extraire_valeur({}) is None


class TestBuildPromql:
    def test_contient_namespace_et_fenetre(self):
        promql = handlers.build_promql("team-a", "10m")
        assert "team-a" in promql
        assert "[10m]" in promql
        assert "min_over_time" in promql


class TestDoitTraiter:
    def test_signature_differente_si_spec_change(self):
        spec = {"costThreshold": 10, "evaluationWindow": "10m"}
        a_traiter, signature = handlers.doit_traiter(spec, "")
        assert a_traiter is True
        assert signature != ""

    def test_pas_de_traitement_si_signature_identique(self):
        spec = {"costThreshold": 10, "evaluationWindow": "10m"}
        _, signature = handlers.doit_traiter(spec, "")
        a_traiter, _ = handlers.doit_traiter(spec, signature)
        assert a_traiter is False

    def test_signature_independante_de_l_ordre_des_cles(self):
        spec_a = {"costThreshold": 10, "evaluationWindow": "10m"}
        spec_b = {"evaluationWindow": "10m", "costThreshold": 10}
        _, sig_a = handlers.doit_traiter(spec_a, "")
        _, sig_b = handlers.doit_traiter(spec_b, "")
        assert sig_a == sig_b


class TestEntryBuilders:
    def test_entry_action_contient_les_infos_cles(self):
        entree = handlers.entry_action(15.0, 10, "10m", "test-workload", 1, priority=2, total_actions=2)
        assert "test-workload" in entree
        assert "1 replicas" in entree
        assert "priorite 2" in entree

    def test_entry_rollback_contient_les_infos_cles(self):
        entree = handlers.entry_rollback("test-workload", 3)
        assert "test-workload" in entree
        assert "3 replicas" in entree
        assert "rollback" in entree.lower()


# ---------------------------------------------------------------------------
# get_deployment_replicas / scale_deployment - gestion d'erreur k8s
# ---------------------------------------------------------------------------

class TestGetDeploymentReplicas:
    def test_retourne_les_replicas_si_succes(self):
        fake_client = Mock()
        fake_client.read_namespaced_deployment.return_value = SimpleNamespace(spec=SimpleNamespace(replicas=3))
        assert handlers.get_deployment_replicas("team-a", "test-workload", apps_client=fake_client) == 3

    def test_retourne_none_si_deployment_introuvable(self):
        fake_client = Mock()
        fake_client.read_namespaced_deployment.side_effect = ApiException(status=404)
        assert handlers.get_deployment_replicas("team-a", "inexistant", apps_client=fake_client) is None


class TestScaleDeployment:
    def test_retourne_true_si_succes_et_utilise_le_bon_field_manager(self):
        fake_client = Mock()
        succes = handlers.scale_deployment("team-a", "test-workload", 1, apps_client=fake_client)
        assert succes is True
        fake_client.patch_namespaced_deployment_scale.assert_called_once_with(
            name="test-workload",
            namespace="team-a",
            body={"spec": {"replicas": 1}},
            field_manager="finops-operator",
        )

    def test_retourne_false_si_echec_api(self):
        fake_client = Mock()
        fake_client.patch_namespaced_deployment_scale.side_effect = ApiException(status=403)
        assert handlers.scale_deployment("team-a", "test-workload", 1, apps_client=fake_client) is False


# ---------------------------------------------------------------------------
# evaluate_and_act - declenchement et garde-fou (cible unique)
# ---------------------------------------------------------------------------

class TestDeclenchementCibleUnique:
    def test_cycle_1_sur_3_ne_declenche_pas_d_action(self, monkeypatch):
        status = {}
        patch, scale_mock = run(base_spec(costThreshold=10), status, monkeypatch)

        assert patch.status["consecutiveExceedances"] == 1
        assert patch.status["thresholdExceeded"] is True
        assert "actionsState" not in patch.status
        scale_mock.assert_not_called()

    def test_cycle_3_sur_3_declenche_le_scale_down(self, monkeypatch):
        status = {"consecutiveExceedances": 2}
        patch, scale_mock = run(
            base_spec(costThreshold=10), status, monkeypatch,
            deployment_replicas={"test-workload": 3},
        )

        assert patch.status["consecutiveExceedances"] == 3
        assert patch.status["actionsState"]["test-workload"]["actionState"] == "ACTION_TAKEN"
        assert patch.status["actionsState"]["test-workload"]["preActionState"] == {"replicas": 3}
        assert patch.status["lastActionAtCycle"] == 3
        assert len(patch.status["correctiveActionTaken"]) == 1
        scale_mock.assert_called_once_with("team-a", "test-workload", 1)

    def test_action_deja_active_ne_re_declenche_pas_le_scale(self, monkeypatch):
        status = {
            "consecutiveExceedances": 10,
            "lastActionAtCycle": 3,
            "actionsState": {"test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}}},
        }
        # consecutive(11) - lastActionAtCycle(3) = 8 >= 3 -> pret a escalader,
        # mais aucune cible NORMAL disponible (une seule cible, deja active).
        _, scale_mock = run(base_spec(costThreshold=10), status, monkeypatch)
        scale_mock.assert_not_called()

    def test_seuil_confirme_sans_action_configuree_logue_sans_agir(self, monkeypatch):
        status = {"consecutiveExceedances": 2}
        _, scale_mock = run(base_spec(costThreshold=10, actions=[]), status, monkeypatch)
        scale_mock.assert_not_called()


# ---------------------------------------------------------------------------
# evaluate_and_act - escalade multi-cibles
# ---------------------------------------------------------------------------

class TestEscalade:
    def test_premiere_cible_priorite_1_declenchee_avant_priorite_2(self, monkeypatch):
        status = {"consecutiveExceedances": 2}
        patch, scale_mock = run(
            two_target_spec(costThreshold=10), status, monkeypatch,
            deployment_replicas={"batch-secondaire": 2, "test-workload": 3},
        )

        # Seule la cible priorite 1 doit avoir ete scalee.
        scale_mock.assert_called_once_with("team-a", "batch-secondaire", 0)
        assert patch.status["actionsState"]["batch-secondaire"]["actionState"] == "ACTION_TAKEN"
        assert "test-workload" not in patch.status["actionsState"]

    def test_ordre_priorite_respecte_meme_si_spec_desordonnee(self, monkeypatch):
        spec = base_spec(
            costThreshold=10,
            actions=[
                {"target": "test-workload", "minReplicas": 1, "priority": 2},
                {"target": "batch-secondaire", "minReplicas": 0, "priority": 1},
            ],
        )
        status = {"consecutiveExceedances": 2}
        _, scale_mock = run(
            spec, status, monkeypatch,
            deployment_replicas={"batch-secondaire": 2, "test-workload": 3},
        )
        # Priorite 1 (batch-secondaire) doit passer en premier malgre
        # son ordre d'apparition en 2eme position dans la spec.
        scale_mock.assert_called_once_with("team-a", "batch-secondaire", 0)

    def test_escalade_vers_priorite_2_apres_nouvelle_confirmation(self, monkeypatch):
        """Priorite 1 deja active depuis le cycle 3 (lastActionAtCycle=3).
        Au cycle 6, on a de nouveau attendu 3 cycles depuis la derniere
        action -> escalade vers priorite 2."""
        status = {
            "consecutiveExceedances": 5,
            "lastActionAtCycle": 3,
            "actionsState": {"batch-secondaire": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 2}}},
        }
        patch, scale_mock = run(
            two_target_spec(costThreshold=10), status, monkeypatch,
            deployment_replicas={"test-workload": 3},
        )

        assert patch.status["consecutiveExceedances"] == 6
        scale_mock.assert_called_once_with("team-a", "test-workload", 1)
        assert patch.status["actionsState"]["test-workload"]["actionState"] == "ACTION_TAKEN"
        # batch-secondaire doit rester present et INCHANGE dans le patch :
        # Kubernetes remplace actionsState en entier a chaque patch, donc
        # les cibles non modifiees doivent y figurer aussi pour ne pas
        # etre perdues (ce n'est pas une fusion cle par cle automatique).
        assert patch.status["actionsState"]["batch-secondaire"]["actionState"] == "ACTION_TAKEN"
        assert patch.status["lastActionAtCycle"] == 6

    def test_pas_d_escalade_avant_nouvelle_confirmation(self, monkeypatch):
        """Priorite 1 active depuis le cycle 3. Au cycle 4 (seulement 1
        cycle depuis la derniere action), pas encore d'escalade."""
        status = {
            "consecutiveExceedances": 3,
            "lastActionAtCycle": 3,
            "actionsState": {"batch-secondaire": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 2}}},
        }
        _, scale_mock = run(two_target_spec(costThreshold=10), status, monkeypatch)
        scale_mock.assert_not_called()

    def test_toutes_les_cibles_actives_ne_re_declenche_rien(self, monkeypatch):
        status = {
            "consecutiveExceedances": 10,
            "lastActionAtCycle": 6,
            "actionsState": {
                "batch-secondaire": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 2}},
                "test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}},
            },
        }
        _, scale_mock = run(two_target_spec(costThreshold=10), status, monkeypatch)
        scale_mock.assert_not_called()


# ---------------------------------------------------------------------------
# evaluate_and_act - rollback
# ---------------------------------------------------------------------------

class TestRollback:
    def test_rollback_normal_quand_replicas_encore_au_minimum(self, monkeypatch):
        status = {
            "actionsState": {"test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}}},
            "correctiveActionTaken": ["entree precedente"],
        }
        patch, scale_mock = run(
            base_spec(costThreshold=10), status, monkeypatch,
            cost_value=2.0, deployment_replicas={"test-workload": 1},
        )

        scale_mock.assert_called_once_with("team-a", "test-workload", 3)
        assert patch.status["actionsState"]["test-workload"]["actionState"] == "NORMAL"
        assert len(patch.status["correctiveActionTaken"]) == 2

    def test_rollback_idempotent_si_deja_effectif(self, monkeypatch):
        """Cas reel rencontre en prod : ArgoCD (ou un cycle precedent) a
        deja remis le Deployment a l'etat pre-action. Ne doit PAS etre
        traite comme un conflit."""
        status = {
            "actionsState": {"test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}}},
            "correctiveActionTaken": ["entree precedente"],
        }
        patch, scale_mock = run(
            base_spec(costThreshold=10), status, monkeypatch,
            cost_value=2.0, deployment_replicas={"test-workload": 3},
        )

        scale_mock.assert_not_called()
        assert patch.status["actionsState"]["test-workload"]["actionState"] == "NORMAL"
        assert len(patch.status["correctiveActionTaken"]) == 2

    def test_vrai_conflit_detecte_si_etat_inattendu(self, monkeypatch):
        status = {
            "actionsState": {"test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}}},
            "correctiveActionTaken": ["entree precedente"],
        }
        patch, scale_mock = run(
            base_spec(costThreshold=10), status, monkeypatch,
            cost_value=2.0, deployment_replicas={"test-workload": 7},  # ni 1 ni 3
        )

        scale_mock.assert_not_called()
        assert "actionsState" not in patch.status  # reste ACTION_TAKEN, pas re-ecrit
        assert "correctiveActionTaken" not in patch.status

    def test_rollback_simultane_de_deux_cibles(self, monkeypatch):
        """Les deux cibles sont ACTION_TAKEN, le cout redescend sous le
        seuil en une seule fois : les deux doivent etre restaurees dans
        le meme cycle, avec les deux entrees d'audit."""
        status = {
            "actionsState": {
                "batch-secondaire": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 2}},
                "test-workload": {"actionState": "ACTION_TAKEN", "preActionState": {"replicas": 3}},
            },
            "correctiveActionTaken": ["entree 1", "entree 2"],
        }
        patch, scale_mock = run(
            two_target_spec(costThreshold=10), status, monkeypatch,
            cost_value=2.0, deployment_replicas={"batch-secondaire": 0, "test-workload": 1},
        )

        assert scale_mock.call_count == 2
        assert patch.status["actionsState"]["batch-secondaire"]["actionState"] == "NORMAL"
        assert patch.status["actionsState"]["test-workload"]["actionState"] == "NORMAL"
        assert len(patch.status["correctiveActionTaken"]) == 4

    def test_aucune_action_si_deja_normal(self, monkeypatch):
        status = {"consecutiveExceedances": 0}
        _, scale_mock = run(base_spec(costThreshold=10), status, monkeypatch, cost_value=2.0)
        scale_mock.assert_not_called()


class TestPasAssezDeDonnees:
    def test_aucun_patch_si_pas_de_metrique(self, monkeypatch):
        status = {}
        patch, scale_mock = run(base_spec(), status, monkeypatch, cost_value=None)
        assert patch.status == {}
        scale_mock.assert_not_called()
