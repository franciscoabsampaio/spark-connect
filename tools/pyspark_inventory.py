#!/usr/bin/env python3
"""
Emit a Python API inventory as JSON.

Usage:
    pip install 'pyspark[connect]==3.5.4'
    python tools/pyspark_inventory.py \\
        --package pyspark.sql \\
        --version-from pyspark \\
        > parity/pyspark-3.5.json

Walks every submodule of each --package, collects every public class
defined there, and lists each class's public members. Classes are keyed
by their full path: `module.QualName`. So `pyspark.sql.session.SparkSession`
and `pyspark.sql.connect.session.SparkSession` are distinct entries.
Nested classes (e.g. `SparkSession.Builder`) inherit the qualname dot.
"""

import argparse
import importlib
import inspect
import json
import pkgutil
import sys

_OBJECT_MEMBERS = frozenset(name for name, _ in inspect.getmembers(object))
# Subclasses of these are "data shapes", not API surface.
_DATA_BASES = (tuple, list, dict, BaseException)


def preload_submodules(package_name: str) -> None:
    """Import every submodule of `package_name` so they show up in sys.modules."""
    try:
        pkg = importlib.import_module(package_name)
    except Exception:
        return
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        return
    for _, mod_name, _ in pkgutil.walk_packages(pkg_path, prefix=package_name + "."):
        try:
            importlib.import_module(mod_name)
        except Exception:
            pass


def _raw_attr(cls: type, name: str) -> object:
    """Return the raw descriptor for `name`, walking the MRO. Skips descriptor invocation."""
    for klass in cls.__mro__:
        if name in klass.__dict__:
            return klass.__dict__[name]
    return None


def _is_method_or_property(cls: type, name: str, value: object) -> bool:
    """Keep methods and properties (including property subclasses like
    pyspark's `classproperty`). Drop class-level data attributes and nested
    classes."""
    raw = _raw_attr(cls, name)
    if isinstance(raw, property):
        return True
    if isinstance(value, type):
        return False
    return callable(value)


def public_api(cls: type) -> list[str]:
    return sorted(
        name
        for name, value in inspect.getmembers(cls)
        if not name.startswith("_")
        and name not in _OBJECT_MEMBERS
        and _is_method_or_property(cls, name, value)
    )


def is_api_class(obj: object) -> bool:
    return isinstance(obj, type) and not issubclass(obj, _DATA_BASES)


def collect_class(cls: type, out: dict[str, list[str]]) -> None:
    """Record `cls` and recurse into its nested classes."""
    full = f"{cls.__module__}.{cls.__qualname__}"
    if full in out:
        return
    out[full] = public_api(cls)
    for name, obj in inspect.getmembers(cls):
        if name.startswith("_") or not is_api_class(obj):
            continue
        # Only recurse into classes defined inside this one.
        if obj.__module__ != cls.__module__:
            continue
        if not obj.__qualname__.startswith(cls.__qualname__ + "."):
            continue
        collect_class(obj, out)


def discover(packages: list[str]) -> dict[str, list[str]]:
    for pkg in packages:
        preload_submodules(pkg)

    result: dict[str, list[str]] = {}
    for mod_name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        if not any(mod_name == p or mod_name.startswith(p + ".") for p in packages):
            continue
        for name, obj in inspect.getmembers(mod):
            if name.startswith("_") or not is_api_class(obj):
                continue
            # Skip re-exports: only record a class in its defining module.
            if obj.__module__ != mod_name:
                continue
            collect_class(obj, result)

    return dict(sorted(result.items()))


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--package", action="append", required=True,
                    help="Package to scan (repeatable).")
    ap.add_argument("--version-from", default=None,
                    help="Module to read `__version__` from.")
    ap.add_argument("-o", "--output", default=None,
                    help="Output file (default: stdout).")
    args = ap.parse_args()

    version = None
    if args.version_from:
        version = getattr(importlib.import_module(args.version_from), "__version__", None)

    inventory = {
        "version": version,
        "packages": args.package,
        "classes": discover(args.package),
    }

    out = open(args.output, "w") if args.output else sys.stdout
    json.dump(inventory, out, indent=2)
    out.write("\n")
    if args.output:
        out.close()


if __name__ == "__main__":
    main()
