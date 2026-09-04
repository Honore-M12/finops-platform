"""
Tests pytest pour l'opérateur FinOpsPolicy.

Organisation :
  - Fonctions pures (evaluate_cost, doit_traiter, entry_*, extraire_valeur,
    build_promql, min_cost) : tests directs, sans mock.
  - evaluate_and_act (coeur de la logique) : tests par scenario, avec
    query_prometheus / get_deployment_replicas / scale_deployment mockes
    via monkeypatch - aucun appel reseau ni cluster reel necessaire.
  - get_deployment_replicas / scale_deployment : tests unitaires avec un
    apps_client factice (unittest.mock), pour verifier la gestion
    d'erreur ApiException independamment du reste.
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
    """Simule l'objet `patch` fourni par Kopf : patch.status[...] = ... et
    patch.metadata.annotations[...] = ... doivent fonctionner comme sur de
    simples dicts, ce que SimpleNamespace + dict permet directement."""
    return SimpleNamespace(status={}, metadata=SimpleNamespace(annotations={}))


def base_spec(**overrides):
    spec = {
        "costThreshold": 10,
        "evaluationWindow": "10m",
        "targetDeployment": "test-workload",
        "minReplicas": 1,
        "consecutiveCyclesThreshold": 3,
    }
    spec.update(overrides)
    return spec


# ---------------------------------------------------------------------------
# Fonctions pures - evaluation
# ---------------------------------------------------------------------------

class TestEvaluateCost:
    def test_pas_assez_de_donnees_si_aucune_valeur(self):
        assert handlers.evaluate_cost(None, 10) == "PAS_ASSEZ_DE_DONNEES"

    def test_depassement_si_strictement_superieur_au_seuil(self):
        assert handlers.evaluate_cost(15, 10) == "DEPASSEMENT"

    def test_ok_si_egal_au_seuil(self):
        # Egal au seuil = pas de depassement (strictement >, pas >=)
        assert handlers.evaluate_cost(10, 10) == "OK"

    def test_ok_si_inferieur_au_seuil(self):
        assert handlers.evaluate_cost(5, 10) == "OK"


class TestMinCost:
    def test_retourne_le_minimum_dans_la_fenetre(self):
        series = [(0, 5.0), (5, 2.0), (10, 8.0)]
        assert handlers.min_cost(series, window_seconds=20, now=10) == 2.0

    def test_ignore_les_valeurs_hors_fenetre(self):
        series = [(0, 1.0), (100, 9.0)]
        # A now=100 avec une fenetre de 10s, le point a t=0 est hors fenetre
        assert handlers.min_cost(series, window_seconds=10, now=100) == 9.0

    def test_retourne_none_si_aucune_valeur_dans_la_fenetre(self):
        series = [(0, 1.0)]
        assert handlers.min_cost(series, window_seconds=5, now=100) is None


class TestExtraireValeur:
    def test_extrait_la_valeur_prometheus(self):
        reponse = {"data": {"result": [{"value": [1234567890, "0.0034259125"]}]}}
        assert handlers.extraire_valeur(reponse) == pytest.approx(0.0034259125)

    def test_retourne_none_si_resultat_vide(self):
        reponse = {"data": {"result": []}}
        assert handlers.extraire_valeur(reponse) is None

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
        # dict(spec) + sort_keys=True doit rendre la signature stable
        # peu importe l'ordre d'insertion des champs.
        spec_a = {"costThreshold": 10, "evaluationWindow": "10m"}
        spec_b = {"evaluationWindow": "10m", "costThreshold": 10}
        _, sig_a = handlers.doit_traiter(spec_a, "")
        _, sig_b = handlers.doit_traiter(spec_b, "")
        assert sig_a == sig_b


class TestEntryBuilders:
    def test_entry_action_contient_les_infos_cles(self):
        entree = handlers.entry_action(15.0, 10, "10m", "test-workload", 1)
        assert "test-workload" in entree
        assert "1 replicas" in entree
        assert "15.0" in entree
        assert "10" in entree

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
        fake_client.read_namespaced_deployment.return_value = SimpleNamespace(
            spec=SimpleNamespace(replicas=3)
        )
        resultat = handlers.get_deployment_replicas("team-a", "test-workload", apps_client=fake_client)
        assert resultat == 3

    def test_retourne_none_si_deployment_introuvable(self):
        fake_client = Mock()
        fake_client.read_namespaced_deployment.side_effect = ApiException(status=404)
        resultat = handlers.get_deployment_replicas("team-a", "inexistant", apps_client=fake_client)
        assert resultat is None


class TestScaleDeployment:
    def test_retourne_true_si_succes(self):
        fake_client = Mock()
        succes = handlers.scale_deployment("team-a", "test-workload", 1, apps_client=fake_client)
        assert succes is True
        fake_client.patch_namespaced_deployment_scale.assert_called_once_with(
            name="test-workload",
            namespace="team-a",
            body={"spec": {"replicas": 1}},
        )

    def test_retourne_false_si_echec_api(self):
        fake_client = Mock()
        fake_client.patch_namespaced_deployment_scale.side_effect = ApiException(status=403)
        succes = handlers.scale_deployment("team-a", "test-workload", 1, apps_client=fake_client)
        assert succes is False


# ---------------------------------------------------------------------------
# evaluate_and_act - coeur de la logique, par scenario
# ---------------------------------------------------------------------------

class TestEvaluateAndActDepassement:
    def test_cycle_1_sur_3_ne_declenche_pas_d_action(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 15.0)
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {}
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        assert patch.status["consecutiveExceedances"] == 1
        assert patch.status["thresholdExceeded"] is True
        assert "actionState" not in patch.status
        scale_mock.assert_not_called()

    def test_cycle_3_sur_3_declenche_le_scale_down(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 15.0)
        monkeypatch.setattr(handlers, "get_deployment_replicas", lambda ns, t: 3)
        scale_mock = Mock(return_value=True)
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {"consecutiveExceedances": 2, "actionState": "NORMAL"}
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        assert patch.status["consecutiveExceedances"] == 3
        assert patch.status["actionState"] == "ACTION_TAKEN"
        assert patch.status["preActionState"] == {"replicas": 3}
        assert len(patch.status["correctiveActionTaken"]) == 1
        scale_mock.assert_called_once_with("team-a", "test-workload", 1)

    def test_action_deja_active_ne_re_declenche_pas_le_scale(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 15.0)
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {"consecutiveExceedances": 10, "actionState": "ACTION_TAKEN"}
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_not_called()
        assert "actionState" not in patch.status  # inchange, donc pas re-ecrit

    def test_seuil_confirme_sans_target_configure_logue_sans_agir(self, monkeypatch, caplog):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 15.0)
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {"consecutiveExceedances": 2, "actionState": "NORMAL"}
        spec = base_spec(costThreshold=10, targetDeployment=None, minReplicas=None)
        handlers.evaluate_and_act("team-a", spec, status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_not_called()
        assert "actionState" not in patch.status


class TestEvaluateAndActRollback:
    def test_rollback_normal_quand_replicas_encore_au_minimum(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 2.0)
        monkeypatch.setattr(handlers, "get_deployment_replicas", lambda ns, t: 1)  # = minReplicas
        scale_mock = Mock(return_value=True)
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {
            "actionState": "ACTION_TAKEN",
            "preActionState": {"replicas": 3},
            "correctiveActionTaken": ["entree precedente"],
        }
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_called_once_with("team-a", "test-workload", 3)
        assert patch.status["actionState"] == "NORMAL"
        assert patch.status["preActionState"] is None
        assert len(patch.status["correctiveActionTaken"]) == 2

    def test_rollback_idempotent_si_deja_effectif(self, monkeypatch):
        """Cas reel rencontre en prod : ArgoCD (ou un cycle precedent) a
        deja remis le Deployment a l'etat pre-action avant que l'operateur
        ne le fasse lui-meme. Ne doit PAS etre traite comme un conflit."""
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 2.0)
        monkeypatch.setattr(handlers, "get_deployment_replicas", lambda ns, t: 3)  # = preActionState deja
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {
            "actionState": "ACTION_TAKEN",
            "preActionState": {"replicas": 3},
            "correctiveActionTaken": ["entree precedente"],
        }
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_not_called()  # deja au bon etat, pas de scale inutile
        assert patch.status["actionState"] == "NORMAL"
        assert patch.status["preActionState"] is None
        assert len(patch.status["correctiveActionTaken"]) == 2

    def test_vrai_conflit_detecte_si_etat_inattendu(self, monkeypatch):
        """Ni l'etat post-action (minReplicas) ni l'etat pre-action
        (preActionState) : intervention externe probable, pas de rollback
        automatique par-dessus une modification non tracee."""
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 2.0)
        monkeypatch.setattr(handlers, "get_deployment_replicas", lambda ns, t: 7)  # ni 1 ni 3
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {
            "actionState": "ACTION_TAKEN",
            "preActionState": {"replicas": 3},
            "correctiveActionTaken": ["entree precedente"],
        }
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_not_called()
        assert "actionState" not in patch.status  # reste ACTION_TAKEN, pas re-ecrit
        assert "correctiveActionTaken" not in patch.status  # pas de nouvelle entree

    def test_aucune_action_si_deja_normal(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: 2.0)
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {"actionState": "NORMAL", "consecutiveExceedances": 0}
        handlers.evaluate_and_act("team-a", base_spec(costThreshold=10), status, patch, handlers.logging.getLogger("test"))

        scale_mock.assert_not_called()
        assert patch.status["consecutiveExceedances"] == 0
        assert patch.status["thresholdExceeded"] is False


class TestEvaluateAndActPasAssezDeDonnees:
    def test_aucun_patch_si_pas_de_metrique(self, monkeypatch):
        monkeypatch.setattr(handlers, "query_prometheus", lambda promql: {})
        monkeypatch.setattr(handlers, "extraire_valeur", lambda r: None)
        scale_mock = Mock()
        monkeypatch.setattr(handlers, "scale_deployment", scale_mock)

        patch = make_patch()
        status = {}
        handlers.evaluate_and_act("team-a", base_spec(), status, patch, handlers.logging.getLogger("test"))

        assert patch.status == {}
        scale_mock.assert_not_called()
