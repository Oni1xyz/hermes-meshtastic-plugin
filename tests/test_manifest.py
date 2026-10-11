import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_manifest_is_a_meshtastic_platform_plugin():
    manifest = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))
    project_text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project_version = re.search(r'^version = "([^"]+)"$', project_text, re.MULTILINE)
    assert manifest["name"] == "meshtastic"
    assert manifest["kind"] == "platform"
    assert project_version is not None
    assert manifest["version"] == project_version.group(1)
    assert manifest["author"] == "Oni1xyz"
    names = {
        item["name"] if isinstance(item, dict) else item
        for item in manifest["requires_env"]
    }
    assert "MESHTASTIC_ALLOWED_USERS" in names
    optional = {
        item["name"] if isinstance(item, dict) else item
        for item in manifest["optional_env"]
    }
    assert {
        "MESHTASTIC_USB_SERIAL",
        "MESHTASTIC_PORT",
        "MESHTASTIC_HOME_NODE",
    } <= optional


def test_readme_documents_safe_setup_and_dependency_install():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "pip install" in text
    assert "hermes plugins install Oni1xyz/hermes-meshtastic-plugin" in text
    assert "MESHTASTIC_ALLOWED_USERS" in text
    assert "MESHTASTIC_USB_SERIAL" in text
    assert "direct message" in text.lower()
    assert "broadcast" in text.lower()


def test_test_bootstrap_has_no_machine_specific_absolute_path():
    text = (ROOT / "tests/conftest.py").read_text(encoding="utf-8")
    assert "/Users/gama" not in text
    assert "HERMES_AGENT_SOURCE" in text


def test_public_repository_metadata_is_present():
    for name in ("LICENSE", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md"):
        assert (ROOT / name).is_file()
