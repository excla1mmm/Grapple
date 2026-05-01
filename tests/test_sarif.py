import gzip
import json
from base64 import b64decode

from conftest import make_finding
from app.sarif import _rule_id, compress_sarif, findings_to_sarif


# --- _rule_id ---

def test_rule_id_incident_action():
    assert _rule_id(make_finding(is_high_risk=True)) == "incident-action"


def test_rule_id_unpinned_action():
    assert _rule_id(make_finding(is_high_risk=False)) == "unpinned-action"


def test_rule_id_reusable_workflow():
    assert _rule_id(make_finding(kind="reusable_workflow")) == "reusable-workflow-unpinned"


def test_rule_id_permission():
    assert _rule_id(make_finding(kind="permission")) == "excess-permissions"


def test_rule_id_runner():
    assert _rule_id(make_finding(kind="runner")) == "self-hosted-runner"


# --- findings_to_sarif ---

def test_sarif_version():
    assert findings_to_sarif([])["version"] == "2.1.0"


def test_sarif_tool_name():
    sarif = findings_to_sarif([])
    assert sarif["runs"][0]["tool"]["driver"]["name"] == "grapple"


def test_safe_findings_excluded():
    sarif = findings_to_sarif([make_finding(severity="safe")])
    assert sarif["runs"][0]["results"] == []


def test_critical_maps_to_error():
    sarif = findings_to_sarif([make_finding(severity="critical")])
    assert sarif["runs"][0]["results"][0]["level"] == "error"


def test_high_maps_to_error():
    sarif = findings_to_sarif([make_finding(severity="high")])
    assert sarif["runs"][0]["results"][0]["level"] == "error"


def test_medium_maps_to_warning():
    sarif = findings_to_sarif([make_finding(severity="medium")])
    assert sarif["runs"][0]["results"][0]["level"] == "warning"


def test_info_maps_to_note():
    sarif = findings_to_sarif([make_finding(severity="info")])
    assert sarif["runs"][0]["results"][0]["level"] == "note"


def test_result_contains_message():
    f = make_finding(message="This action is unpinned.")
    sarif = findings_to_sarif([f])
    assert sarif["runs"][0]["results"][0]["message"]["text"] == "This action is unpinned."


def test_result_contains_workflow_location():
    f = make_finding(workflow_file=".github/workflows/ci.yml")
    sarif = findings_to_sarif([f])
    loc = sarif["runs"][0]["results"][0]["locations"][0]["physicalLocation"]
    assert loc["artifactLocation"]["uri"] == ".github/workflows/ci.yml"


def test_mixed_findings_only_actionable_included():
    findings = [
        make_finding(severity="critical"),
        make_finding(severity="safe"),
        make_finding(severity="medium"),
    ]
    sarif = findings_to_sarif(findings)
    assert len(sarif["runs"][0]["results"]) == 2


# --- compress_sarif ---

def test_compress_sarif_roundtrip():
    sarif = findings_to_sarif([make_finding(severity="critical")])
    b64 = compress_sarif(sarif)
    recovered = json.loads(gzip.decompress(b64decode(b64)))
    assert recovered["version"] == "2.1.0"
    assert len(recovered["runs"][0]["results"]) == 1


def test_compress_sarif_returns_ascii_string():
    b64 = compress_sarif(findings_to_sarif([]))
    assert isinstance(b64, str)
    assert b64.isascii()
