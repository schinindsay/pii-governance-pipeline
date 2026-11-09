"""Apply a human review decision to an existing PII finding."""

import argparse

import duckdb

from data_platform.platform.config import load_settings
from data_platform.platform.warehouse.duckdb import connect_warehouse
from data_platform.projects.pii_scanner.core.review import review_finding


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("decision", choices=("accept", "reject"))
    parser.add_argument("model", help="Registry DBT_MODEL")
    parser.add_argument("field", help="Registry FIELD")
    parser.add_argument("--reviewed-by", required=True, help="Reviewer identity")
    parser.add_argument("--notes", help="Optional reason for the decision")
    args = parser.parse_args()

    try:
        with connect_warehouse(load_settings().warehouse_path) as con:
            changed = review_finding(
                con,
                args.model,
                args.field,
                decision=args.decision,
                reviewed_by=args.reviewed_by,
                notes=args.notes,
            )
    except (ValueError, RuntimeError, OSError, duckdb.Error) as error:
        parser.exit(1, f"{error}\n")

    status = "accepted" if args.decision == "accept" else "rejected"
    if changed:
        print(f"{args.model}.{args.field}: {status}")
    else:
        print(f"{args.model}.{args.field}: already {status}")


if __name__ == "__main__":
    main()
