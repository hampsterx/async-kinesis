#!/usr/bin/env python3
"""
Check that an environment holds the lowest versions async-kinesis declares.

Usage:
    python scripts/check_dependency_floors.py --extras kpl,redis,msgpack

The minimum-dependencies CI job installs with `uv pip install --resolution lowest-direct`
and then runs this check. A resolver can still settle above a declared floor (another
extra pins a dependency higher, or the floor release is yanked), and the suite would then
pass against versions the floor never promised. This script fails in that case.
"""

import argparse
import sys
from importlib import metadata

from packaging.requirements import Requirement
from packaging.version import Version

PACKAGE = "async-kinesis"


def declared_floors(requirements, extras):
    """Return {name: floor} for the core requirements plus the selected extras"""
    floors = {}
    for line in requirements:
        req = Requirement(line)
        # The "" stands in for no extra, so a core requirement with an environment marker still counts
        if req.marker is not None and not any(req.marker.evaluate({"extra": extra}) for extra in extras or [""]):
            continue
        floor = [spec.version for spec in req.specifier if spec.operator in (">=", "==")]
        if len(floor) != 1:
            raise ValueError(f"{line!r} has no single >= or == floor to check")
        floors[req.name] = floor[0]
    return floors


def find_mismatches(floors, installed_version):
    """Return (name, floor, installed) for each floor the environment does not hold exactly"""
    mismatches = []
    for name, floor in sorted(floors.items()):
        try:
            installed = installed_version(name)
        except metadata.PackageNotFoundError:
            installed = None
        if installed is None or Version(installed) != Version(floor):
            mismatches.append((name, floor, installed))
    return mismatches


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check installed versions equal declared lower bounds")
    parser.add_argument("--extras", default="", help="Comma-separated extras that were installed")
    args = parser.parse_args(argv)

    extras = [extra for extra in args.extras.split(",") if extra]
    dist = metadata.distribution(PACKAGE)
    unknown = sorted(set(extras) - set(dist.metadata.get_all("Provides-Extra") or []))
    if unknown:
        print(f"Unknown extras: {', '.join(unknown)}", file=sys.stderr)
        return 2

    floors = declared_floors(dist.requires or [], extras)
    mismatches = find_mismatches(floors, metadata.version)
    failed = {name for name, _, _ in mismatches}
    for name, floor in sorted(floors.items()):
        if name not in failed:
            print(f"ok   {name}=={floor}")
    for name, floor, installed in mismatches:
        print(f"FAIL {name}: declared floor {floor}, installed {installed}", file=sys.stderr)
    if mismatches:
        return 1
    print(f"All {len(floors)} declared floors installed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
