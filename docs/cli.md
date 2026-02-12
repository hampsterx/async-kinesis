# CLI Reference

The `async-kinesis` CLI provides commands for interacting with Kinesis streams from the terminal. It uses the library's Consumer and Producer classes directly, giving you the same deaggregation, serialization, rate limiting, and reconnection logic as the Python API.

## Installation

```bash
pip install async-kinesis[cli]
```

This installs Click as a dependency and registers the `async-kinesis` console script.

## Global Options

```
async-kinesis [OPTIONS] COMMAND [ARGS]...
```

| Option | Env Var | Description |
| --- | --- | --- |
| `--endpoint-url` | `ENDPOINT_URL` | Kinesis endpoint URL (for LocalStack, kinesalite, etc.) |
| `--region` | `AWS_DEFAULT_REGION` | AWS region name |
| `-v, --verbose` | | Enable debug logging (shows library internals) |
| `--version` | | Show version and exit |

AWS credentials are read from the standard boto3 chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, etc.).

## Commands

### `list`

List Kinesis streams in the account/region.

```bash
async-kinesis list [--limit N]
```

| Option | Default | Description |
| --- | --- | --- |
| `--limit` | 100 | Maximum number of streams to return |

Handles both AWS (StreamSummaries with status/shard count/mode) and kinesalite (StreamNames-only) response formats.

**Example:**
```
$ async-kinesis list
Name         Status  Shards
-----------  ------  ------
my-stream    ACTIVE  2
events       ACTIVE  4
```

### `describe`

Show details about a specific stream including shard information.

```bash
async-kinesis describe STREAM
```

**Example:**
```
$ async-kinesis describe my-stream
  Name        my-stream
  ARN         arn:aws:kinesis:ap-southeast-2:123456789:stream/my-stream
  Status      ACTIVE
  Retention   24 hours
  Encryption  NONE
  Shards      1

Shard ID              Status  Start Hash  End Hash                                 Start Seq   End Seq
--------------------  ------  ----------  ---------------------------------------  ----------  -------
shardId-000000000000  OPEN    0           340282366920938463463374607431768211455  49635...
```

### `tail`

Tail records from a stream. Uses a real Consumer with MemoryCheckPointer for stateless tailing.

```bash
async-kinesis tail STREAM [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `-i, --iterator-type` | `LATEST` | Where to start: `LATEST`, `TRIM_HORIZON`, `AT_TIMESTAMP` |
| `-f, --format` | `json-pretty` | Output format: `json-pretty`, `json`, `raw`, `raw-short` |
| `-p, --processor` | `json` | Record processor: `json`, `string` |
| `-n, --max-records` | *(unlimited)* | Stop after N records |

**Output formats:**

| Format | Description |
| --- | --- |
| `json-pretty` | Indented JSON (default) |
| `json` | Compact single-line JSON (good for piping to `jq`) |
| `raw` | Python `repr()` of the deserialized record |
| `raw-short` | Python `repr()` truncated to 120 characters |

**Examples:**

```bash
# Live tail (latest records, Ctrl+C to stop)
async-kinesis tail my-stream

# Read from beginning, stop after 10
async-kinesis tail my-stream -i TRIM_HORIZON -n 10

# Compact JSON for piping
async-kinesis tail my-stream -i TRIM_HORIZON -f json -n 100 | jq '.user_id'

# Read raw strings (for StringProcessor data)
async-kinesis tail my-stream -p string -f raw -n 5
```

### `put`

Put records into a stream. Uses a real Producer with full batching and flushing.

```bash
async-kinesis put STREAM [DATA] [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `-k, --partition-key` | *(auto-generated)* | Explicit partition key |
| `-p, --processor` | `json` | Record processor: `json`, `string` |
| `--create` | off | Create the stream if it does not exist |

**Input modes:**
- **Argument**: `async-kinesis put my-stream '{"key": "value"}'` — single record
- **Stdin**: `cat data.jsonl | async-kinesis put my-stream` — one record per line

When reading from a TTY without a pipe, a hint is printed to stderr.

**Examples:**

```bash
# Single JSON record
async-kinesis put my-stream '{"event": "page_view", "url": "/home"}'

# With partition key
async-kinesis put my-stream '{"user": 123}' -k user-123

# Create stream if needed
async-kinesis put --create new-stream '{"first": "record"}'

# Pipe JSONL file
cat events.jsonl | async-kinesis put my-stream

# Generate records
seq 5 | jq -c '{n: .}' | async-kinesis put my-stream

# String processor (no JSON parsing)
async-kinesis put my-stream "plain text message" -p string
```

## LocalStack / Kinesalite Usage

```bash
export ENDPOINT_URL=http://localhost:4566    # LocalStack
export AWS_DEFAULT_REGION=ap-southeast-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

async-kinesis list
async-kinesis put --create test-stream '{"hello": "world"}'
async-kinesis tail test-stream -i TRIM_HORIZON -n 1
```

Or pass the endpoint directly:

```bash
async-kinesis --endpoint-url http://localhost:4567 list
```
