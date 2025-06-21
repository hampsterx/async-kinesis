#!/usr/bin/env python3
"""
Release script for async-kinesis

Usage:
    python scripts/release.py --version 1.2.0
    python scripts/release.py --check  # Check current version
    python scripts/release.py --next-patch  # Auto-increment patch version
    python scripts/release.py --next-minor  # Auto-increment minor version
    python scripts/release.py --next-major  # Auto-increment major version
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def get_current_version():
    """Extract current version from setup.py"""
    setup_py = Path("setup.py")
    if not setup_py.exists():
        raise FileNotFoundError("setup.py not found")

    content = setup_py.read_text()
    match = re.search(r'version="([^"]+)"', content)
    if not match:
        raise ValueError("Version not found in setup.py")

    return match.group(1)


def update_version(new_version):
    """Update version in setup.py"""
    setup_py = Path("setup.py")
    content = setup_py.read_text()

    # Update version in setup.py
    new_content = re.sub(r'version="[^"]+"', f'version="{new_version}"', content)

    if content == new_content:
        raise ValueError("Version pattern not found in setup.py")

    setup_py.write_text(new_content)
    print(f"Updated setup.py version to {new_version}")


def increment_version(current_version, bump_type):
    """Increment version based on bump type"""
    parts = current_version.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid version format: {current_version}")

    major, minor, patch = map(int, parts)

    if bump_type == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump_type == "minor":
        minor += 1
        patch = 0
    elif bump_type == "patch":
        patch += 1
    else:
        raise ValueError(f"Invalid bump type: {bump_type}")

    return f"{major}.{minor}.{patch}"


def run_tests():
    """Run tests to ensure everything works before release"""
    print("Running tests...")
    result = subprocess.run(["python", "-m", "pytest", "-x"], capture_output=True, text=True)
    if result.returncode != 0:
        print("Tests failed!")
        print(result.stdout)
        print(result.stderr)
        return False
    print("Tests passed!")
    return True


def run_linting():
    """Run linting checks"""
    print("Running linting checks...")

    # Black
    result = subprocess.run(["black", "--check", "kinesis", "tests"], capture_output=True)
    if result.returncode != 0:
        print("Black formatting check failed!")
        return False

    # isort
    result = subprocess.run(["isort", "--check-only", "kinesis", "tests"], capture_output=True)
    if result.returncode != 0:
        print("isort check failed!")
        return False

    # flake8
    result = subprocess.run(
        [
            "flake8",
            "kinesis",
            "tests",
            "--max-line-length=120",
            "--extend-ignore=E203,W503,E501",
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        print("flake8 check failed!")
        return False

    print("All linting checks passed!")
    return True


def create_tag(version):
    """Create and push git tag"""
    print(f"Creating git tag {version}...")

    # Check if working directory is clean
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if result.stdout.strip():
        print("Working directory is not clean. Please commit changes first.")
        return False

    # Create tag
    subprocess.run(["git", "tag", version], check=True)
    print(f"Created tag {version}")

    # Ask user if they want to push
    push = input("Push tag to origin? (y/N): ").lower().strip()
    if push in ("y", "yes"):
        subprocess.run(["git", "push", "origin", version], check=True)
        print(f"Pushed tag {version} to origin")
        print(f"GitHub Actions will now build and publish version {version} to PyPI")
    else:
        print(f"Tag {version} created locally. Push manually with: git push origin {version}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Release management for async-kinesis")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--version", help="Set specific version")
    group.add_argument("--check", action="store_true", help="Check current version")
    group.add_argument("--next-patch", action="store_true", help="Increment patch version")
    group.add_argument("--next-minor", action="store_true", help="Increment minor version")
    group.add_argument("--next-major", action="store_true", help="Increment major version")

    parser.add_argument("--skip-tests", action="store_true", help="Skip running tests")
    parser.add_argument("--skip-lint", action="store_true", help="Skip linting checks")
    parser.add_argument("--no-tag", action="store_true", help="Don't create git tag")

    args = parser.parse_args()

    try:
        current_version = get_current_version()
        print(f"Current version: {current_version}")

        if args.check:
            return

        # Determine new version
        if args.version:
            new_version = args.version
        elif args.next_patch:
            new_version = increment_version(current_version, "patch")
        elif args.next_minor:
            new_version = increment_version(current_version, "minor")
        elif args.next_major:
            new_version = increment_version(current_version, "major")
        else:
            print("Error: Must specify either --version, --next-patch, --next-minor, or --next-major")
            sys.exit(1)

        print(f"New version: {new_version}")

        # Run checks
        if not args.skip_lint and not run_linting():
            sys.exit(1)

        if not args.skip_tests and not run_tests():
            sys.exit(1)

        # Update version
        update_version(new_version)

        # Create tag if requested
        if not args.no_tag:
            if not create_tag(new_version):
                sys.exit(1)
        else:
            print(f"Version updated to {new_version}. Create tag manually with: git tag {new_version}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
