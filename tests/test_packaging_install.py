from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"


def _load_pyproject() -> dict:
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)


def _venv_paths(venv_dir: Path) -> tuple[Path, Path]:
    if os.name == "nt":
        scripts_dir = venv_dir / "Scripts"
        return scripts_dir / "python.exe", scripts_dir
    scripts_dir = venv_dir / "bin"
    return scripts_dir / "python", scripts_dir


def test_pyproject_declares_console_scripts() -> None:
    project = _load_pyproject()["project"]
    scripts = project["scripts"]
    assert scripts["vault-ops"] == "vault_ops_cli:main"
    assert scripts["vault-agent"] == "vault_agent_cli:main"
    assert scripts["redaction-eval"] == "redaction_eval_harness:main"


def test_setuptools_exposes_script_modules_from_scripts_dir() -> None:
    setuptools_cfg = _load_pyproject()["tool"]["setuptools"]
    assert setuptools_cfg["package-dir"][""] == "scripts"
    py_modules = set(setuptools_cfg["py-modules"])
    assert {
        "redaction_eval_harness",
        "vault_ops_cli",
        "vault_agent_cli",
        "vault_sources",
        "vault_vector_index",
    } <= py_modules


def test_manual_validation_doc_is_explicitly_manual() -> None:
    content = (ROOT / "docs" / "manual-openclaw-agent-validation.md").read_text(encoding="utf-8")
    assert "manual" in content.lower()
    assert "operator-run" in content.lower()
    assert "does not mean release validation is complete" in content


def test_editable_install_exposes_console_scripts(tmp_path: Path) -> None:
    venv_dir = tmp_path / "venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
        check=True,
        cwd=ROOT,
    )
    venv_python, scripts_dir = _venv_paths(venv_dir)

    env = dict(os.environ)
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    subprocess.run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--no-build-isolation",
            "-e",
            str(ROOT),
        ],
        check=True,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    vault_ops = scripts_dir / ("vault-ops.exe" if os.name == "nt" else "vault-ops")
    vault_agent = scripts_dir / ("vault-agent.exe" if os.name == "nt" else "vault-agent")
    redaction_eval = scripts_dir / ("redaction-eval.exe" if os.name == "nt" else "redaction-eval")

    ops = subprocess.run(
        [str(vault_ops), "--help"],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    agent = subprocess.run(
        [str(vault_agent), "--help"],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    eval_cmd = subprocess.run(
        [str(redaction_eval), "--help"],
        check=True,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    assert "usage: vault-ops" in ops.stdout
    assert "migrate-encryption" in ops.stdout
    assert "usage: vault-agent" in agent.stdout
    assert "search-redacted" in agent.stdout
    assert "usage: redaction-eval" in eval_cmd.stdout
    assert "--require-llm-candidates" in eval_cmd.stdout
