"""Pure, conservative dbt YAML masking plan from human-accepted PII records.

This is not an automatic approval or masking deployment. Review the resulting PR.
"""

from .dbt_yaml_edit import ALREADY_SET, APPLIED, CREATED, add_masking_policy


def build_masking_plan(
    accepted, manifest_models, read_file, *, default_policy="pii_hash_string_mask"
):
    by_model = {str(m["model"]).lower(): m for m in manifest_models}
    files, applied, already_set, flagged = {}, [], [], []
    for row in accepted:
        model, column = str(row["model"]), str(row["field"])
        meta = by_model.get(model.lower())
        materialization = str(
            row.get("materialization") or (meta or {}).get("materialization") or ""
        ).lower()
        if materialization not in {"table", "incremental"}:
            flagged.append(
                {
                    "model": model,
                    "field": column,
                    "reason": "view_or_unsupported_materialization",
                }
            )
            continue
        path = (meta or {}).get("patch_path")
        if not path:
            flagged.append(
                {"model": model, "field": column, "reason": "missing_dbt_yaml_path"}
            )
            continue
        if path.startswith("/") or ".." in path.split("/") or "\\" in path:
            flagged.append(
                {"model": model, "field": column, "reason": "unsafe_dbt_yaml_path"}
            )
            continue
        try:
            original = files[path] if path in files else read_file(path)
            edited, status = add_masking_policy(
                original, model, column, row.get("policy") or default_policy
            )
        except (OSError, ValueError, KeyError) as exc:
            flagged.append(
                {"model": model, "field": column, "reason": type(exc).__name__}
            )
            continue
        if status in (APPLIED, CREATED):
            files[path] = edited
            applied.append({"model": model, "field": column, "path": path})
        elif status == ALREADY_SET:
            already_set.append({"model": model, "field": column, "path": path})
        else:
            flagged.append(
                {"model": model, "field": column, "path": path, "reason": status}
            )
    return {
        "files": files,
        "applied": applied,
        "already_set": already_set,
        "flagged": flagged,
    }
