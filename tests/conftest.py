import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from app.analyzer import Finding


@pytest.fixture(scope="session")
def rsa_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def make_finding(**kwargs) -> Finding:
    defaults = {
        "kind": "action",
        "workflow_file": ".github/workflows/ci.yml",
        "action_name": "actions/checkout",
        "ref": "v4",
        "pin_type": "tag",
        "severity": "medium",
        "message": "Test message.",
        "is_high_risk": False,
        "is_pinned": False,
    }
    defaults.update(kwargs)
    return Finding(**defaults)
