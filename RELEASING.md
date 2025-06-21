# Release Process

This document describes the automated release process for async-kinesis.

## Quick Release

For a patch release (1.1.5 → 1.1.6):
```bash
python scripts/release.py --next-patch
```

For a minor release (1.1.5 → 1.2.0):
```bash
python scripts/release.py --next-minor
```

For a major release (1.1.5 → 2.0.0):
```bash
python scripts/release.py --next-major
```

For a specific version:
```bash
python scripts/release.py --version 1.2.3
```

## What Happens

1. **Pre-flight checks**: The script runs linting and tests
2. **Version update**: Updates the version in `setup.py`
3. **Git tag creation**: Creates and optionally pushes a git tag
4. **Automated deployment**: GitHub Actions detects the tag and:
   - Runs full test suite across Python 3.10, 3.11, 3.12
   - Runs linting checks
   - Builds the package
   - Publishes to PyPI using trusted publishing

## Manual Process

If you prefer to do it manually:

1. Update version in `setup.py`
2. Commit the version change
3. Create a git tag: `git tag 1.2.3`
4. Push the tag: `git push origin 1.2.3`
5. GitHub Actions will handle the rest

## PyPI Configuration

The deployment uses GitHub's trusted publishing feature, which requires:

1. Configure the PyPI project to trust this GitHub repository
2. Set up the `pypi` environment in GitHub repository settings
3. The workflow uses `id-token: write` permission for OIDC authentication

## Checking Current Version

```bash
python scripts/release.py --check
```

## Development Releases

For testing the release process without affecting the main package:

```bash
# Skip tests and linting for quick iteration
python scripts/release.py --version 1.2.3-dev --skip-tests --skip-lint --no-tag
```

## Troubleshooting

- **Tests fail**: Fix the issues before releasing
- **Linting fails**: Run `black kinesis tests` and `isort kinesis tests`
- **PyPI upload fails**: Check repository settings and trusted publishing configuration
- **Tag already exists**: Delete the tag locally and remotely if needed:
  ```bash
  git tag -d 1.2.3
  git push origin :refs/tags/1.2.3
  ```
