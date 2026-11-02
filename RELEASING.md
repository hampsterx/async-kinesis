# Release Process

This document describes the automated release process for async-kinesis.

Releases are driven by bare `X.Y.Z` git tags, without a `v` prefix. The `version` in
`setup.py` is a placeholder: the publish workflow overwrites it from the tag at build
time, so a release never commits a version bump.

## Quick Release

Run these from an up-to-date, clean `master` with tags fetched (`git fetch --tags`), after
the Tests workflow has passed on that commit. The script reads the latest release from
the tags.

### Before you run it

The script calls `python`, `black`, `isort` and `flake8` by name, so run it with a virtualenv
on `PATH` that has the package and its test dependencies installed. `venv/` is gitignored,
so it does not dirty the tree the script checks:

```bash
python3 -m venv venv
venv/bin/pip install -e . -r test-requirements.txt black isort flake8
```

Integration tests skip without Kinesis and Redis endpoints, and a run with them skipped
still prints "Tests passed!". Start both so the pre-flight tests exercise them:

```bash
docker compose up -d kinesis redis
export PATH="$PWD/venv/bin:$PATH" REDIS_HOST=localhost REDIS_PORT=16379
```

When it has created the tag, the script asks `Push tag to origin? (y/N)`. Answering `y`
starts the publish workflow.

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
   - Calls the Tests workflow (`test.yml`), the same jobs every pull request passes: the suite across Python 3.11 through 3.15, the declared dependency minimums, the Docker run and linting
   - Once all of those pass, the `publish` job waits for a required reviewer to approve
     the `pypi` environment. Approve it from the Publish run's page in the Actions tab;
     rejecting it uploads nothing
   - After approval, writes the tag version into `setup.py`, builds the package, and
     publishes to PyPI using trusted publishing
5. **GitHub release**: written by hand once PyPI serves the version (see below)

## After the Upload

Check that PyPI serves the version (`https://pypi.org/pypi/async-kinesis/X.Y.Z/json`
returns the wheel and sdist; the project-level JSON can lag by a few minutes), then
create the GitHub release for the tag:

```bash
gh release create X.Y.Z --verify-tag --title X.Y.Z --notes-file notes.md
```

The notes follow the shape of earlier releases: a `## What's changed` list with one
bullet per user-visible change, in terms of what a caller sees, ending with the PR
number; a `## Behaviour changes` list when callers may need to act (smaller batches, a
new exception); and a `**Full changelog**:` compare link from the previous tag. CI-only
changes share one bullet. `git log --oneline <previous tag>..X.Y.Z` lists the PRs.

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
- **Tests fail on the pushed tag**: the `publish` job never starts, so nothing reaches
  PyPI and the version number is still free. Fix the cause on `master` through a pull
  request, wait for the Tests workflow to pass there, then delete the tag (below) and
  run the release again from the new `master`. 3.0.1 took this route: its first tag
  failed before upload and was recreated on the fixed commit
- **Tag is wrong but tests passed**: reject the `pypi` approval, then delete the tag.
  Once PyPI has a version it cannot be replaced, so a version that was uploaded by
  mistake needs the next patch number instead
- **Linting fails**: Run `black kinesis tests` and `isort kinesis tests`
- **PyPI upload fails**: Check repository settings and trusted publishing configuration
- **Tag already exists**: Delete the tag locally and remotely if needed:
  ```bash
  git tag -d 3.0.1
  git push origin :refs/tags/3.0.1
  ```
