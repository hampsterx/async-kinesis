Guidance for AI coding agents working in the async-kinesis repository.

This file defines repository-specific operating rules for autonomous or semi-autonomous coding agents. Follow these instructions unless a maintainer explicitly tells you otherwise.

---

## Project Overview

`async-kinesis` is an AsyncIO-based Python library for producing to and consuming from AWS Kinesis Data Streams. It is published to PyPI as `async-kinesis` and consumed as a library by downstream services.

- Python: 3.10 - 3.14
- Core deps: `aiobotocore`, `async-timeout`, `asyncio-throttle`
- Optional extras: `redis`, `dynamodb`, `prometheus`, `kpl`, `msgpack`, `cli`, `testing`

---

## First Principles

When making changes, follow these priorities:

1. Correctness over convenience
2. Keep the public API stable (this is a library; downstream consumers depend on it)
3. Add tests for new bugs, edge cases, and behaviour changes
4. Narrow, focused changes; avoid drive-by refactors
5. Prefer editing existing code over introducing new abstractions

Critical rules:

- Do not change public method signatures or exception types without a clear deprecation story
- Do not add backwards-compatibility shims for features that never shipped; just change the code
- Do not silently swallow exceptions, specifically in checkpointer / producer / consumer paths
- Document behaviour changes in the PR body (Summary + Backwards compatibility). Release notes are generated from tagged GitHub releases; there is no `CHANGELOG.md`

---

## Package Layout

```text
kinesis/
  base.py            # Shared async client setup and aiobotocore lifecycle
  producer.py        # Producer: batching, aggregation, flush
  consumer.py        # Consumer: shard iteration, checkpointing hook, metrics
  checkpointers.py   # MemoryCheckPointer, RedisCheckPointer
  dynamodb.py        # DynamoDBCheckPointer
  aggregators.py     # KPL aggregation helpers
  serializers.py     # JSON / msgpack serializers
  processors.py      # Record post-processing hooks
  metrics.py         # Metrics collector interface
  prometheus.py      # Prometheus adapter for metrics
  exceptions.py      # Public exception types
  testing.py         # In-memory mock Producer/Consumer + pytest plugin
  timeout_compat.py  # async-timeout / asyncio.timeout shim across Python versions
  utils.py           # Shared helpers
  cli/               # Optional CLI (install with [cli] extra)

tests/               # Integration + unit tests (Floci + redis for integration)
docs/                # User-facing documentation
```

Rule: copy an existing pattern before introducing a new one. `consumer.py` and `producer.py` share lifecycle conventions from `base.py`; checkpointer implementations mirror each other.

---

## Known Quirks

- **aiobotocore teardown** (`base.py` `__aexit__`): aiobotocore >= 3.x can raise `AssertionError("Session was never entered")` during `__aexit__` if the underlying HTTP session was never used. The known message is caught and logged at debug; unexpected assertions log at warning and are swallowed to let cleanup finish. Do not tighten this without verifying against aiobotocore 2.x and 3.x.
- **Checkpointer buffer semantics**: `manual_checkpoint()` in both `RedisCheckPointer` and `DynamoDBCheckPointer` is best-effort. Every buffered shard is attempted on each call; per-shard failures are collected into `CheckpointFlushError.errors` instead of aborting on the first raise. Failing shards stay buffered for retry; successful shards are popped individually after the backend write. Do not reintroduce clear-before-iterate behaviour.
- **DynamoDB endpoint propagation**: `dynamodb.py` propagates `endpoint_url` to all resource and client calls so Floci / LocalStack tests work end-to-end. Any new boto3 client creation in this module must carry `endpoint_url` through the same code path.

---

## Testing Conventions

- **Integration backend**: Floci 1.5.4 on port 4566. This is used by both `docker-compose` and CI. There is no kinesalite path.
- **Redis**: required for `RedisCheckPointer` tests. Docker compose exposes it on port 16379.
- **DynamoDB tests**: marked `dynamodb`. There is no auto-skip; the quick-loop command in `CONTRIBUTING.md` excludes them with `-m "not dynamodb"`, and the CI job includes them.
- **Mock backend**: `kinesis.testing` exposes `MockProducer` / `MockConsumer` plus a pytest plugin registered via `entry_points["pytest11"]`. Use these for unit tests that do not need a real wire protocol. Prefer them over stubbing `aiobotocore` directly.

Pytest markers (declarative tags; no automatic deselection):

- `slow` - long-running tests
- `integration` - hits Floci / real services
- `aws` - requires real AWS credentials (opt-in by setting `TESTING_USE_AWS_KINESIS=1`)
- `redis` - requires redis
- `dynamodb` - requires DynamoDB backend

Running tests: see `CONTRIBUTING.md` § Running Tests for exact commands.

---

## Linting and Formatting

See [CONTRIBUTING.md](./CONTRIBUTING.md) § Code Style for linting, formatting, and pre-commit rules.

---

## Release Awareness

- Releases are tag-driven via GitHub Actions. Push a `vX.Y.Z` tag and the publish workflow builds sdist/wheel and uploads to PyPI.
- The `version` field in `setup.py` is overwritten by CI from the git tag at build time; do not hand-edit it as part of a feature PR.
- Release notes live on the GitHub release (tag), not in a `CHANGELOG.md` file. Describe behaviour changes in the PR body so they can be picked up when the release is cut.

---

## Pull Request Guidelines

- Branch names: `fix/`, `feat/`, `refactor/`, `docs/`, `chore/` prefix, kebab-case
- Target branch: `master`
- PR description: see `CONTRIBUTING.md` § Pull Requests for the required sections (Summary, Backwards compatibility, Test plan)
- Write for the reviewer and downstream consumer, not the implementor
- Do not reference internal working docs, plans, or step numbers in commits or PR text

Conventional commit prefixes are used in merge titles:

- `feat:` / `fix:` / `perf:` / `refactor:` / `docs:` / `chore:` / `test:`

Do not add `Co-Authored-By` trailers or agent footers to commits.

---

## Agent Filename Hint

`AGENTS.md` is the canonical agent instructions file for this repository. If your coding agent expects a different filename (some tools look for `CLAUDE.md`, `GEMINI.md`, `COPILOT.md`), create a local symlink rather than copying the file so both stay in sync:

```bash
ln -s AGENTS.md CLAUDE.md
ln -s AGENTS.md GEMINI.md
ln -s AGENTS.md COPILOT.md
```

Note: the maintainer's personal `CLAUDE.md` is gitignored in this repo, so a clone starts without one. The symlink above is safe to create in a fresh clone.

---

## Before Finishing

1. Run the relevant tests (unit suite at minimum; integration if the change touches wire behaviour or checkpointers)
2. Run `pre-commit run --all-files` or let the install hook do it
3. Confirm the PR description has a **Backwards compatibility** section describing any user-visible impact

---

## Common Mistakes

- Forgetting the **Backwards compatibility** section in the PR (this is a library; it is required)
- Skipping tests for a behaviour change because "it is obvious"
- Reading real AWS/boto3 internals when `kinesis.testing` would isolate the unit under test
- Editing `setup.py` `version` as part of a feature PR
- Introducing a new backwards-compatibility shim for a feature that never shipped instead of just changing the code
- Adding `Co-Authored-By` trailers to commits
