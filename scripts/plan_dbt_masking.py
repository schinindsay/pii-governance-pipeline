"""Offline preview of dbt YAML masking changes; does not publish or apply policies."""

import argparse
import json
from pathlib import Path

from data_platform.projects.pii_scanner.core.dbt_masking_plan import build_masking_plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "accepted_json", type=Path, help="Explicitly human-accepted PII records"
    )
    p.add_argument(
        "manifest_models_json",
        type=Path,
        help="dbt model metadata: model, patch_path, materialization",
    )
    p.add_argument("dbt_checkout", type=Path, help="Local checkout of the dbt repo")
    p.add_argument(
        "--output", type=Path, help="Optional JSON plan output (may contain dbt YAML)"
    )
    args = p.parse_args()
    root = args.dbt_checkout.resolve()

    def read_file(path):
        target = (root / path).resolve()
        if not target.is_relative_to(root):
            raise ValueError("Path escapes dbt checkout")
        return target.read_text(encoding="utf-8")

    plan = build_masking_plan(
        json.loads(args.accepted_json.read_text(encoding="utf-8")),
        json.loads(args.manifest_models_json.read_text(encoding="utf-8")),
        read_file,
    )
    summary = {key: value for key, value in plan.items() if key != "files"}
    summary["changed_paths"] = sorted(plan["files"])
    print(json.dumps(summary, indent=2))
    if args.output:
        args.output.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
