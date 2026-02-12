import asyncio
import functools
import json
import logging
import sys

import click

from kinesis import exceptions


def async_command(f):
    """Decorator that wraps an async click command with asyncio.run() and common error handling."""

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return asyncio.run(f(*args, **kwargs))
        except KeyboardInterrupt:
            sys.exit(130)
        except exceptions.StreamDoesNotExist as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
        except json.JSONDecodeError as e:
            click.echo(f"Error: invalid JSON: {e}", err=True)
            sys.exit(1)

    return wrapper


def format_table(headers, rows):
    """Format a list of rows as a padded text table."""
    if not rows:
        return ""

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    # Build format string
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*headers)]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append(fmt.format(*[str(c) for c in row]))

    return "\n".join(lines)


def format_kv(pairs):
    """Format key-value pairs with aligned values."""
    if not pairs:
        return ""
    max_key = max(len(k) for k, _ in pairs)
    lines = []
    for key, value in pairs:
        lines.append(f"  {key:<{max_key}}  {value}")
    return "\n".join(lines)


@click.group()
@click.option("--endpoint-url", envvar="ENDPOINT_URL", default=None, help="Kinesis endpoint URL.")
@click.option("--region", envvar="AWS_DEFAULT_REGION", default=None, help="AWS region name.")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose (debug) logging.")
@click.version_option(package_name="async-kinesis")
@click.pass_context
def main(ctx, endpoint_url, region, verbose):
    """async-kinesis CLI — interact with Amazon Kinesis streams."""
    ctx.ensure_object(dict)
    ctx.obj["endpoint_url"] = endpoint_url
    ctx.obj["region"] = region

    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# Import commands to register them with the group
from kinesis.cli.stream import describe, list_streams, put, tail  # noqa: E402

main.add_command(describe)
main.add_command(list_streams, "list")
main.add_command(tail)
main.add_command(put)
