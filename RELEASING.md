# Release Process

This document describes the automated release process for async-kinesis.

Releases are driven by bare `X.Y.Z` git tags, without a `v` prefix. The `version` in
`setup.py` is a placeholder: the publish workflow overwrites it from the tag at build
time, so a release never commits a version bump.

## Quick Release

Run these from an up-to-date, clean `master` with tags fetched (`git fetch --tags`). The
script reads the latest release from the tags.

For a patch release (3.0.0 → 3.0.1):
```bash
python scripts/release.py --next-patch
```

For a minor release (3.0.0 → 3.1.0):
```bash
python scripts/release.py --next-minor
```

For a major release (3.0.0 → 4.0.0):
```bash
python scripts/release.py --next-major
```

For a specific version, which must be newer than the latest release tag:
```bash
python scripts/release.py --version 3.0.1
```

## What Happens

1. **Version check**: Refuses a version that is not bare `X.Y.Z` or is not newer than the latest release tag
2. **Pre-flight checks**: Runs linting and tests
3. **Git tag creation**: Creates the tag on a clean working tree and optionally pushes it
4. **Automated deployment**: GitHub Actions detects the tag and:
   - Runs the full test suite across Python 3.11 through 3.15
   - Runs linting checks
   - Writes the tag version into `setup.py` and builds the package
   - Publishes to PyPI using trusted publishing

## Manual Process

If you prefer to do it manually:

1. Create a git tag: `git tag 3.0.1`. Use a bare `X.Y.Z` newer than the latest release: the publish workflow accepts any tag and writes it into `setup.py` unchecked, so this path skips the helper's validation
2. Push the tag: `git push origin 3.0.1`
3. GitHub Actions will handle the rest

## PyPI Configuration

The deployment uses GitHub's trusted publishing feature, which requires:

1. Configure the PyPI project to trust this GitHub repository
2. Set up the `pypi` environment in GitHub repository settings
3. The workflow uses `id-token: write` permission for OIDC authentication

## Checking the Latest Release

```bash
python scripts/release.py --check
```

## Dry Run

To run the version check, linting and tests without creating a tag:

```bash
python scripts/release.py --next-patch --no-tag
```

## Troubleshooting

- **Tests fail**: Fix the issues before releasing
- **Linting fails**: Run `black kinesis tests` and `isort kinesis tests`
- **PyPI upload fails**: Check repository settings and trusted publishing configuration
- **Tag already exists**: Delete the tag locally and remotely if needed:
  ```bash
  git tag -d 3.0.1
  git push origin :refs/tags/3.0.1
  ```
