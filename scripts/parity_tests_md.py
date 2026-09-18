"""Render an api-parity reference inventory of a test suite as a checklist.

Ticks already present in the output file are carried over, so regenerating
after a re-vendor does not wipe the record of what has been ported.
"""

import json
import re
import sys
from pathlib import Path

TICKED = re.compile(r"^- \[x\] `?([\w.]+)`?", re.MULTILINE)


def previously_ticked(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(TICKED.findall(path.read_text()))


def main(inventory: Path, output: Path) -> None:
    data = json.loads(inventory.read_text())
    ticked = previously_ticked(output)

    by_class: dict[str, list[str]] = {}
    for entry in data["entries"]:
        if entry["kind"] != "method":
            continue
        cls, _, name = entry["path"].rpartition(".")
        by_class.setdefault(cls, []).append(name)

    names = [n for group in by_class.values() for n in group]
    done = sum(1 for n in names if n in ticked)
    version = data.get("version") or "unknown"

    lines = [
        "# Vendored upstream tests",
        "",
        f"Reference: `{data['source']}` (spark v{version})",
        "",
        "Ports of Apache Spark's own test bodies. Regenerate with "
        "`make parity-tests`; ticks are preserved across runs.",
        "",
        f"**{done}/{len(names)} ported.**",
    ]

    for cls, group in sorted(by_class.items()):
        lines += ["", f"## `{cls}`", ""]
        for name in sorted(group):
            lines.append(f"- [{'x' if name in ticked else ' '}] `{name}`")

    output.write_text("\n".join(lines) + "\n")
    print(f"{output}: {done}/{len(names)} ported", file=sys.stderr)


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
