# Contributing to async-kinesis

Thanks for your interest in contributing. This is an open-source library consumed by downstream services, so changes should be narrow, tested, and backwards-compatible wherever possible.

## Ways to Contribute

- **Bug reports**: open a [GitHub issue](https://github.com/hampsterx/async-kinesis/issues) with a minimal reproduction
- **Pull requests**: bug fixes, new features, test coverage, docs
- **Documentation**: see `docs/` for user-facing guides

## Getting Started

### Prerequisites

- Python 3.10 - 3.14
- Docker (for integration tests; spins up Floci and redis)

### Setup

```bash
git clone https://github.com/hampsterx/async-kinesis.git
cd async-kinesis

python -m venv venv
source venv/bin/activate

pip install -e .
pip install -r test-requirements.txt
pre-commit install
```

## Running Tests

Integration tests use [Floci](https://github.com/floci-io/floci) (AWS emulator) on port 4566 and redis on port 16379. Both the local Docker Compose setup and CI use the same images and ports.

Pick one of the two paths below:

### All-in-one (builds the test container)

Runs the full suite inside a container that already has pytest wired up. Good for a one-shot "does it pass?" check.

```bash
docker compose up --abort-on-container-exit --exit-code-from test
```

### Local dev loop (services only; run pytest on the host)

Host-run equivalent: start the backing services in Docker, then iterate with `pytest` against your local venv. Better for a tight edit-test cycle.

```bash
docker compose up -d kinesis redis

ENDPOINT_URL=http://localhost:4566 \
REDIS_HOST=localhost REDIS_PORT=16379 \
AWS_DEFAULT_REGION=ap-southeast-2 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
pytest -v -m "not dynamodb"

docker compose down
```

### Pytest markers

Markers are declarative tags; pytest does not auto-deselect any of them. Use `-m` to scope your run. The quick-loop command above uses `-m "not dynamodb"` to skip that backend.

| Marker | Meaning |
| --- | --- |
| `slow` | Long-running tests |
| `integration` | Requires Floci (Docker) |
| `aws` | Requires real AWS credentials (`TESTING_USE_AWS_KINESIS=1`) |
| `redis` | Requires redis |
| `dynamodb` | DynamoDB-specific; run with `-m dynamodb` |

### DynamoDB tests

```bash
pytest -v -m dynamodb
```

### AWS integration tests (optional)

Some tests can run against real AWS Kinesis. Create a `.env`:

```env
TESTING_USE_AWS_KINESIS=1
```

### Resharding tests

Specialised suite under `tests/resharding/`. See [`tests/resharding/README.md`](./tests/resharding/README.md) for the full matrix (unit, integration, production) and per-file dependencies.

## Code Style

- Line length: 120
- `black`, `isort`, `autoflake`, `flake8` run via pre-commit hooks
- Flake8 ignores: `E203,W503,E712,E402,F401,F841,F541,E501,E722`

```bash
pre-commit run --all-files
```

## Commit Messages

Use conventional prefixes:

- `feat:` new feature
- `fix:` bug fix
- `perf:` performance improvement
- `refactor:` code change without behaviour change
- `test:` test-only changes
- `docs:` documentation
- `chore:` build, CI, dependencies, tooling

Do not add `Co-Authored-By` trailers or agent attribution footers.

## Pull Requests

1. Branch from `master`: `git checkout -b fix/my-bug`
2. Push and open a PR targeting `master`
3. CI must pass before merge

### PR description

Write for the reviewer and downstream consumer, not the implementor. Lead with user-facing impact.

**Required sections:**

- **Summary** - what changed for callers and why
- **Backwards compatibility** - this is a library. Call out any public API, signature, or exception-type change. For pure-internal changes, state "No public API change."
- **Test plan** - `[x]` for done, `[ ]` for remaining. Example:

```markdown
## Test plan
- [x] Unit tests cover new branch
- [x] Integration suite green locally (Floci 1.5.4)
- [ ] Verified against real AWS
```

### Scope

- One feature or fix per PR
- No drive-by refactors unrelated to the stated change
- Describe user-visible impact in the PR body. Release notes are published on the GitHub release, generated from PR titles and bodies; there is no `CHANGELOG.md`

## Release Process (maintainers)

Releases are tag-driven via GitHub Actions:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

The publish workflow builds sdist/wheel and uploads to PyPI. The `version` field in `setup.py` is overwritten from the tag at build time, so there is no need to bump it by hand in PRs. Release notes are written on the GitHub release page (use the "Generate release notes" action to pull in merged PR titles since the previous tag).

## Security

Please do **not** open public issues for security vulnerabilities. Use [GitHub private vulnerability reporting](https://github.com/hampsterx/async-kinesis/security/advisories/new), or email the maintainer.

## Architecture and Agent Operating Rules

For deeper architecture notes, known quirks, and rules for AI coding agents, see [AGENTS.md](./AGENTS.md).
