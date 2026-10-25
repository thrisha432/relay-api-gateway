"""``python -m relay`` / ``relay``: validate config and run the gateway with Uvicorn."""

from __future__ import annotations

import argparse
import sys

import uvicorn

from relay.app import create_app
from relay.config import ConfigError, load_config
from relay.logging_config import configure_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay", description="Relay API gateway")
    parser.add_argument("--config", help="path to routes.yaml (default: $RELAY_CONFIG)")
    parser.add_argument("--host", help="override server.host")
    parser.add_argument("--port", type=int, help="override server.port")
    parser.add_argument("--check", action="store_true", help="validate the config file and exit")
    args = parser.parse_args(argv)

    configure_logging()
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    if args.check:
        print(f"config OK: {len(config.routes)} route(s)")
        return 0

    uvicorn.run(
        create_app(config),
        host=args.host or config.server.host,
        port=args.port or config.server.port,
        log_config=None,
        access_log=False,  # Relay writes its own structured access log
        timeout_graceful_shutdown=int(config.server.shutdown_grace_seconds),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
