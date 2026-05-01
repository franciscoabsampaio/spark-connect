#!/usr/bin/env python3
"""
Cross-check a Rust api-parity dump against a Python inventory.

Usage:
    python tools/parity_report.py \\
        --py   parity/pyspark-3.5.json \\
        --rust parity/spark-connect.json \\
        -o     parity/PARITY.md

Inputs
------
--py    : output of `pyspark_inventory.py` ({version, packages, classes})
--rust  : output of the Rust `api_parity_dump` binary (entries[])

Matching
--------
Each Rust entry's `reference` is split on the last `.` into (class_path, member).
The class_path must be a key in `classes`, and the member must be in
`classes[class_path]`. Anything else is reported as a "stale" reference
(typo or drift between Python version and Rust references).
"""

import argparse
import json
import sys
from collections import defaultdict


def split_ref(ref: str) -> tuple[str, str] | None:
    """Split `Foo.Bar.baz` into (`Foo.Bar`, `baz`). None if no dot."""
    if "." not in ref:
        return None
    head, _, tail = ref.rpartition(".")
    return head, tail


def build_indices(
    rust_entries: list[dict], py_class_keys: set[str],
) -> tuple[dict[str, dict], dict[str, dict[str, dict]]]:
    """
    Partition Rust entries:
      - class_idx[cls_path]            -> entry  (whole reference equals a class key)
      - member_idx[cls_path][member]   -> entry  (rpartition matches a class key)
    Entries that fit neither are left for the stale pass.
    """
    class_idx: dict[str, dict] = {}
    member_idx: dict[str, dict[str, dict]] = defaultdict(dict)
    for e in rust_entries:
        ref = e["reference"]
        if ref in py_class_keys:
            class_idx[ref] = e
            continue
        parts = split_ref(ref)
        if parts is None:
            continue
        cls, member = parts
        member_idx[cls][member] = e
    return class_idx, member_idx


def render_markdown(report: dict) -> str:
    py = report["python"]
    rust = report["rust"]
    out: list[str] = []
    out.append(f"# API parity report")
    out.append("")
    out.append(f"- Python: `{py['source']}` (version `{py['version']}`)")
    out.append(f"- Rust:   `{rust['source']}` (version `{rust['version']}`)")
    out.append("")

    totals = report["totals"]
    out.append("## Summary")
    out.append("")
    out.append(f"- Classes in Python inventory: **{totals['py_classes']}**")
    out.append(f"- Classes declared by Rust: **{totals['classes_covered']}** "
               f"({totals['classes_pct']:.1f}%)")
    out.append(f"- Members in Python inventory: **{totals['py_members']}**")
    out.append(f"- Members covered by Rust: **{totals['covered']}** "
               f"({totals['coverage_pct']:.1f}%)")
    out.append(f"  - implemented: {totals['implemented']}")
    out.append(f"  - partial: {totals['partial']}")
    out.append(f"  - unimplemented: {totals['unimplemented']}")
    out.append(f"- Stale Rust references (no match in Python): "
               f"**{totals['stale']}**")
    out.append("")

    out.append("## Per-class coverage")
    out.append("")
    out.append("| Class | Class status | Python members | Covered | % |")
    out.append("|---|---|---:|---:|---:|")
    for row in report["classes"]:
        cls_status = row["class_status"] or "—"
        out.append(
            f"| `{row['class']}` | {cls_status} | {row['total']} | "
            f"{row['covered']} | {row['pct']:.0f}% |"
        )
    out.append("")

    out.append("## Detail")
    out.append("")
    for row in report["classes"]:
        if row["class_status"] is None and not any(m["status"] for m in row["members"]):
            continue
        out.append(f"### `{row['class']}`")
        out.append("")
        if row["class_status"] is not None:
            impl = row["class_implementation"]
            comment = row["class_comment"] or ""
            out.append(f"- Class status: **{row['class_status']}**"
                       f" (impl `{impl}`)" + (f" — {comment}" if comment else ""))
            out.append("")
        out.append("| Member | Status | Implementation | Comment |")
        out.append("|---|---|---|---|")
        for m in row["members"]:
            status = m["status"] or "—"
            impl = f"`{m['implementation']}`" if m["implementation"] else "—"
            comment = m["comment"] or ""
            out.append(f"| `{m['name']}` | {status} | {impl} | {comment} |")
        out.append("")

    if report["stale"]:
        out.append("## Stale Rust references")
        out.append("")
        out.append("Entries whose `reference` did not resolve in the Python inventory. "
                   "Likely a typo, removed pyspark API, or path-convention mismatch "
                   "(e.g. `SparkSession.builder.appName` instead of "
                   "`SparkSession.Builder.appName`).")
        out.append("")
        out.append("| Reference | Implementation |")
        out.append("|---|---|")
        for s in report["stale"]:
            out.append(f"| `{s['reference']}` | `{s['implementation']}` |")
        out.append("")

    return "\n".join(out)


def build_report(py_path: str, rust_path: str) -> dict:
    with open(py_path) as f:
        py = json.load(f)
    with open(rust_path) as f:
        rust = json.load(f)

    py_classes = py["classes"]
    py_class_keys = set(py_classes.keys())
    class_idx, member_idx = build_indices(rust["entries"], py_class_keys)

    class_rows = []
    covered = implemented = partial = unimplemented = total_members = 0
    classes_covered = 0

    for cls_path, members in py_classes.items():
        class_entry = class_idx.get(cls_path)
        rust_for_cls = member_idx.get(cls_path, {})
        member_rows = []
        cov = 0
        for m in members:
            entry = rust_for_cls.get(m)
            if entry is None:
                member_rows.append({
                    "name": m, "status": None,
                    "implementation": None, "comment": None,
                })
                continue
            cov += 1
            status = entry["status"]
            if status == "implemented":
                implemented += 1
            elif status == "partial":
                partial += 1
            elif status == "unimplemented":
                unimplemented += 1
            member_rows.append({
                "name": m, "status": status,
                "implementation": entry["implementation"],
                "comment": entry.get("comment"),
            })
        covered += cov
        total_members += len(members)
        if class_entry is not None:
            classes_covered += 1
        class_rows.append({
            "class": cls_path,
            "class_status": class_entry["status"] if class_entry else None,
            "class_implementation": class_entry["implementation"] if class_entry else None,
            "class_comment": class_entry.get("comment") if class_entry else None,
            "total": len(members),
            "covered": cov,
            "pct": (100.0 * cov / len(members)) if members else 0.0,
            "members": member_rows,
        })

    # Sort: most-covered first, then alphabetical.
    class_rows.sort(key=lambda r: (-r["pct"], -r["covered"], r["class"]))

    # Stale = Rust references that didn't land in either index.
    matched_refs = set(class_idx.keys())
    for cls, members in member_idx.items():
        py_members_for_cls = set(py_classes.get(cls, ()))
        for member, entry in members.items():
            if member in py_members_for_cls:
                matched_refs.add(entry["reference"])
    stale = [e for e in rust["entries"] if e["reference"] not in matched_refs]

    coverage_pct = (100.0 * covered / total_members) if total_members else 0.0
    classes_pct = (100.0 * classes_covered / len(py_classes)) if py_classes else 0.0

    return {
        "python":  {"source": py_path,   "version": py.get("version")},
        "rust":    {"source": rust_path, "version": rust.get("spark_version")},
        "totals": {
            "py_classes": len(py_classes),
            "classes_covered": classes_covered,
            "classes_pct": classes_pct,
            "py_members": total_members,
            "covered": covered,
            "implemented": implemented,
            "partial": partial,
            "unimplemented": unimplemented,
            "stale": len(stale),
            "coverage_pct": coverage_pct,
        },
        "classes": class_rows,
        "stale": stale,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--py", required=True, help="Python inventory JSON")
    ap.add_argument("--rust", required=True, help="Rust api-parity dump JSON")
    ap.add_argument("-o", "--output", default=None,
                    help="Markdown output (default: stdout)")
    ap.add_argument("--json", default=None,
                    help="Also emit raw report as JSON to this path")
    args = ap.parse_args()

    report = build_report(args.py, args.rust)

    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)

    md = render_markdown(report)
    if args.output:
        with open(args.output, "w") as f:
            f.write(md + "\n")
    else:
        sys.stdout.write(md + "\n")


if __name__ == "__main__":
    main()
