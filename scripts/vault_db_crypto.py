#!/usr/bin/env python3
"""Encrypted DB migration utilities for llm-vault."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from vault_db import migrate_plaintext_to_encrypted


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate plaintext sqlite DBs to SQLCipher-encrypted DBs",
    )
    p.add_argument(
        "--db-path",
        action="append",
        required=True,
        help="Repeatable path to a plaintext sqlite DB to migrate",
    )
    p.add_argument(
        "--backup-suffix",
        default=".plaintext.bak",
        help="Suffix for pre-encryption backups",
    )
    p.add_argument("--json", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    results = []
    failed = 0
    total = len(args.db_path)
    for idx, raw in enumerate(args.db_path, start=1):
        path = Path(str(raw))

        def _emit_progress(db_path: str, stage: str, action: str) -> None:
            if not args.verbose or args.json:
                return
            print(
                "[progress] "
                f"[stage=migrate-encryption.{stage}] "
                f"[item={idx}/{total}] "
                f"[action={action}] "
                f"[db={Path(db_path).name}]"
            )

        if args.verbose and not args.json:
            print(
                "[progress] "
                "[stage=migrate-encryption] "
                f"[item={idx}/{total}] "
                "[action=start] "
                f"[db={path.name}]"
            )
        try:
            results.append(
                migrate_plaintext_to_encrypted(
                    path,
                    backup_suffix=str(args.backup_suffix),
                    progress=_emit_progress,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            results.append(
                {
                    "db_path": str(path),
                    "status": "error",
                    "error": str(exc),
                }
            )
        if args.verbose and not args.json:
            status = str(results[-1].get("status") or "unknown")
            print(
                "[progress] "
                "[stage=migrate-encryption] "
                f"[item={idx}/{total}] "
                f"[action=done status={status}] "
                f"[db={path.name}]"
            )

    payload = {"count": len(results), "failed": failed, "results": results}
    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        for item in results:
            status = str(item.get("status") or "unknown")
            line = f"{item.get('db_path')} status={status}"
            if status == "ok":
                line += (
                    f" backup={item.get('backup_path')} "
                    f"tables_verified={item.get('tables_verified')} "
                    f"rows_verified_total={item.get('rows_verified_total')}"
                )
            elif status == "error":
                line += f" error={item.get('error')}"
            elif status == "skipped":
                line += f" reason={item.get('reason')}"
            print(line)

    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        import sys

        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
