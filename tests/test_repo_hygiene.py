from __future__ import annotations

import re
import stat
import subprocess
from pathlib import Path

TRACKED_RELEASE_PATHS = (
    "README.md",
    "TODO.md",
    "docs",
    "scripts",
    "skills",
    "tests",
    "pyproject.toml",
    "vault-agent",
    "vault-ops",
    "vault-ops.toml.example",
)
PERSONAL_MACHINE_PATH = re.compile(
    r"(/home/[A-Za-z0-9._-]+/|/Users/[A-Za-z0-9._-]+/|[A-Za-z]:\\\\Users\\\\[A-Za-z0-9._ -]+\\\\)"
)


def test_gitignore_covers_runtime_artifacts() -> None:
    content = Path(".gitignore").read_text(encoding="utf-8")
    for expected in [
        "state/*.db",
        "state/*.db-*",
        "state/*.log",
        "logs/*.log",
        "__pycache__/",
        ".pytest_cache/",
        "tmp/",
    ]:
        assert expected in content


def test_shell_scripts_are_executable() -> None:
    script_paths = list(Path("scripts").glob("*.sh")) + [
        Path("vault-agent"),
        Path("vault-ops"),
        Path("skills/vault-unified-local/scripts/vault-unified-cli.sh"),
    ]
    for path in script_paths:
        mode = path.stat().st_mode
        assert mode & stat.S_IXUSR, f"{path} must be executable"


def test_readme_mentions_required_db_password() -> None:
    content = Path("README.md").read_text(encoding="utf-8")
    assert "LLM_VAULT_DB_PASSWORD" in content


def test_tracked_release_files_do_not_contain_personal_machine_paths() -> None:
    output = subprocess.check_output(
        ["git", "ls-files", "--", *TRACKED_RELEASE_PATHS],
        text=True,
    )
    offenders: list[str] = []
    for rel_path in [line.strip() for line in output.splitlines() if line.strip()]:
        path = Path(rel_path)
        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except UnicodeDecodeError:
            continue
        if PERSONAL_MACHINE_PATH.search(content):
            offenders.append(rel_path)
    assert offenders == [], f"personal machine paths found in tracked release files: {offenders}"
