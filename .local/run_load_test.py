#!/usr/bin/env python3
"""Run a private, long-running Elasticsearch -> Edwin poller load test.

This file is intentionally kept under ``.local`` and excluded through
``.git/info/exclude``. It creates synthetic Kibana-style event documents in a
temporary index, runs the current Docker image against them, and removes all
runtime resources on exit.

Example:
    python .local/run_load_test.py --confirm-live --duration 3d
    python .local/run_load_test.py --confirm-live --duration 30m \
        --scenarios steady,burst --steady-rate 2 --batch-size 50
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse, urlunparse

import requests
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCENARIOS = ("steady", "burst", "backlog", "same-timestamp")
SCENARIOS = set(DEFAULT_SCENARIOS)


def parse_duration(value: str) -> float:
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        suffix = value[-1].lower()
        amount = float(value[:-1]) if suffix in units else float(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            "duration must be seconds or a number followed by s, m, h, or d"
        )
    if amount <= 0:
        raise argparse.ArgumentTypeError("duration must be greater than zero")
    return amount * units.get(suffix, 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-live", action="store_true",
                        help="required confirmation that generated events go to Edwin")
    parser.add_argument("--duration", type=parse_duration, default=parse_duration("3d"))
    parser.add_argument("--scenarios", default=",".join(DEFAULT_SCENARIOS),
                        help="comma-separated cycle: steady,burst,backlog,same-timestamp")
    parser.add_argument("--scenario-seconds", type=parse_duration, default=300)
    parser.add_argument("--steady-rate", type=float, default=2.0,
                        help="events per second during steady phases")
    parser.add_argument("--burst-size", type=int, default=1000)
    parser.add_argument("--burst-idle", type=float, default=60.0)
    parser.add_argument("--backlog-rate", type=float, default=100.0)
    parser.add_argument("--batch-size", type=int, default=500,
                        help="events per Elasticsearch bulk request")
    parser.add_argument("--page-size", type=int, default=500,
                        help="poller ELASTIC_BATCH_SIZE")
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--es-url", default=None,
                        help="host Elasticsearch URL; defaults to ELASTIC_URL in .env")
    parser.add_argument("--image", default="edwin-elastic-poller:local-load-test")
    parser.add_argument(
        "--pip-trusted-host",
        action="append",
        default=["pypi.org", "files.pythonhosted.org"],
        help="host allowed without CA verification during the private image build; repeatable",
    )
    parser.add_argument("--keep-index", action="store_true")
    parser.add_argument("--log-file", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="validate configuration and print actions without Docker or ES writes")
    return parser.parse_args()


def load_environment() -> dict[str, str]:
    values = {
        key: value for key, value in dotenv_values(REPO_ROOT / ".env").items()
        if value is not None
    }
    values.update({key: value for key, value in os.environ.items()})
    return values


def require_positive(name: str, value: float | int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def host_gateway_url(host_url: str) -> str:
    parsed = urlparse(host_url)
    host = parsed.hostname
    if host not in {"localhost", "127.0.0.1", "::1"}:
        return host_url.rstrip("/")
    netloc = "host.docker.internal"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse((parsed.scheme, netloc, parsed.path, "", parsed.query, "")).rstrip("/")


def auth_kwargs(environment: dict[str, str]) -> dict[str, object]:
    token = environment.get("ELASTIC_TOKEN")
    if token:
        return {"headers": {"Authorization": f"ApiKey {token}"}}
    user, password = environment.get("ELASTIC_USER"), environment.get("ELASTIC_PASS")
    return {"auth": (user, password)} if user and password else {}


def event_document(number: int, timestamp: str | None = None) -> tuple[str, dict]:
    stamp = timestamp or datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return (
        f"load-{number:016d}",
        {
            "@timestamp": stamp,
            "message": f"elastic-poller load test event {number}",
            "event": {
                "provider": "alerting",
                "action": "load-test",
                "kind": "alert",
                "category": ["logs"],
            },
            "rule": {
                "id": f"load-rule-{number % 100}",
                "name": "elastic-poller-load-test",
                "category": "logs.alert.document.count",
                "license": "basic",
            },
            "kibana": {
                "alert": {"rule": {"rule_type_id": "logs.alert.document.count"}},
                "space_ids": ["load-test"],
            },
        },
    )


def bulk_index(
    es_url: str,
    index: str,
    documents: Iterable[tuple[str, dict]],
    request_kwargs: dict[str, object],
) -> int:
    lines: list[str] = []
    count = 0
    for doc_id, source in documents:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
        lines.append(json.dumps(source))
        count += 1
    if not lines:
        return 0
    response = requests.post(
        f"{es_url}/{index}/_bulk",
        params={"refresh": "wait_for"},
        data=("\n".join(lines) + "\n").encode(),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
        **request_kwargs,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("errors"):
        raise RuntimeError(f"Elasticsearch bulk request contained errors: {json.dumps(body)[:1000]}")
    return count


def create_index(es_url: str, index: str, request_kwargs: dict[str, object]) -> None:
    body = {
        "settings": {"number_of_shards": 3, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "event": {"properties": {
                    "provider": {"type": "keyword"}, "action": {"type": "keyword"},
                    "kind": {"type": "keyword"}, "category": {"type": "keyword"},
                }},
                "rule": {"properties": {
                    "id": {"type": "keyword"}, "name": {"type": "keyword"},
                    "category": {"type": "keyword"}, "license": {"type": "keyword"},
                }},
                "kibana": {"properties": {
                    "space_ids": {"type": "keyword"},
                    "alert": {"properties": {"rule": {"properties": {
                        "rule_type_id": {"type": "keyword"},
                    }}}},
                }},
            }
        },
    }
    response = requests.put(f"{es_url}/{index}", json=body, timeout=30, **request_kwargs)
    response.raise_for_status()


def delete_index(es_url: str, index: str, request_kwargs: dict[str, object]) -> None:
    try:
        requests.delete(f"{es_url}/{index}", timeout=30, **request_kwargs)
    except requests.RequestException as exc:
        print(f"warning: could not delete {index}: {exc}", file=sys.stderr)


def build_image(image: str, trusted_hosts: list[str], data_dir: Path) -> None:
    """Build a local image with an isolated pip certificate workaround.

    Corporate TLS interception can make the stock python image unable to
    verify PyPI's certificate chain. This Dockerfile is generated under
    ``.local`` at runtime; the production Dockerfile is deliberately untouched.
    """
    trusted_flags = " ".join(f"--trusted-host {host}" for host in trusted_hosts)
    dockerfile = data_dir / "Dockerfile"
    dockerfile.write_text(
        f"""FROM python:3.12

WORKDIR /app

COPY pyproject.toml README.md ./
COPY edwin_elastic_poller ./edwin_elastic_poller
RUN pip3 install --no-cache-dir {trusted_flags} .

ENV BOOKMARK_PATH=/data/

CMD ["python3", "-u", "-m", "edwin_elastic_poller"]
""",
        encoding="utf-8",
    )
    subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", image, str(REPO_ROOT)],
        check=True,
        cwd=REPO_ROOT,
    )


def stream_logs(container: str, output: object, stop_event: threading.Event) -> None:
    process = subprocess.Popen(
        ["docker", "logs", "--follow", container],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert process.stdout is not None
        for line in process.stdout:
            print(f"[poller] {line}", end="")
            if output:
                output.write(line)
                output.flush()
            if stop_event.is_set():
                break
    finally:
        if process.poll() is None:
            process.terminate()


def scenario_documents(name: str, number: int, amount: int) -> list[tuple[str, dict]]:
    timestamp = None
    if name == "same-timestamp":
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return [event_document(number + offset, timestamp) for offset in range(amount)]


def run_producer(
    args: argparse.Namespace,
    es_url: str,
    index: str,
    request_kwargs: dict[str, object],
    stop_event: threading.Event,
) -> None:
    started = time.monotonic()
    next_number = 0
    delivered = 0
    scenario_index = 0
    next_report = started + 30
    while not stop_event.is_set() and time.monotonic() - started < args.duration:
        scenario = args.scenarios_list[scenario_index % len(args.scenarios_list)]
        scenario_index += 1
        phase_end = min(started + args.duration, time.monotonic() + args.scenario_seconds)
        phase_started = time.monotonic()
        while not stop_event.is_set() and time.monotonic() < phase_end:
            elapsed = time.monotonic() - phase_started
            if scenario == "steady":
                amount = max(1, min(args.batch_size, round(args.steady_rate)))
                wait = amount / args.steady_rate
            elif scenario == "burst":
                amount = args.burst_size if elapsed < 1 else 0
                wait = args.burst_idle if amount else args.burst_idle
            elif scenario == "backlog":
                producing = elapsed < args.scenario_seconds / 2
                amount = max(1, min(args.batch_size, round(args.backlog_rate))) if producing else 0
                wait = amount / args.backlog_rate if amount else 1
            else:
                amount = args.batch_size
                wait = max(1.0, args.batch_size / max(args.steady_rate, 1))
            if amount:
                count = bulk_index(es_url, index, scenario_documents(scenario, next_number, amount), request_kwargs)
                next_number += count
                delivered += count
            if time.monotonic() >= next_report:
                print(f"[producer] indexed={delivered} scenario={scenario}")
                next_report += 30
            stop_event.wait(wait)


def main() -> int:
    args = parse_args()
    environment = load_environment()
    args.scenarios_list = [item.strip() for item in args.scenarios.split(",") if item.strip()]
    try:
        require_positive("batch-size", args.batch_size)
        require_positive("page-size", args.page_size)
        require_positive("poll-interval", args.poll_interval)
        require_positive("steady-rate", args.steady_rate)
        require_positive("burst-size", args.burst_size)
        require_positive("backlog-rate", args.backlog_rate)
        require_positive("scenario-seconds", args.scenario_seconds)
        if not args.scenarios_list or any(item not in SCENARIOS for item in args.scenarios_list):
            raise ValueError(f"scenarios must be selected from {', '.join(DEFAULT_SCENARIOS)}")
        if not args.confirm_live:
            raise ValueError("--confirm-live is required because this runner delivers to Edwin")
        for name in ("EDWIN_ORG", "EDWIN_ID", "EDWIN_TOKEN"):
            if not environment.get(name):
                raise ValueError(f"{name} is required in .env or the environment")
        host_es_url = (args.es_url or environment.get("ELASTIC_URL") or "").rstrip("/")
        if not host_es_url:
            raise ValueError("--es-url or ELASTIC_URL is required")
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    index = f"elastic-poller-load-{uuid.uuid4().hex[:10]}"
    container = f"elastic-poller-load-{uuid.uuid4().hex[:8]}"
    container_es_url = host_gateway_url(host_es_url)
    request_kwargs = auth_kwargs(environment)
    print(f"index={index} host_es={host_es_url} container_es={container_es_url}")
    print(f"duration={args.duration:.0f}s scenarios={','.join(args.scenarios_list)}")
    if args.dry_run:
        print(
            "dry-run: private Dockerfile uses pip trusted hosts "
            f"{','.join(args.pip_trusted_host)}"
        )
        print(f"dry-run: docker build -t {args.image} .")
        print(f"dry-run: docker run --name {container} ... ELASTIC_INDEXS={index}")
        return 0

    log_handle = args.log_file.open("a", encoding="utf-8") if args.log_file else None
    stop_event = threading.Event()
    data_dir = Path(tempfile.mkdtemp(prefix="elastic-poller-load-"))

    def stop_handler(_signum: int, _frame: object) -> None:
        print("\nshutdown requested; cleaning up...")
        stop_event.set()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    try:
        response = requests.get(f"{host_es_url}/_cluster/health", timeout=10, **request_kwargs)
        response.raise_for_status()
        create_index(host_es_url, index, request_kwargs)
        build_image(args.image, args.pip_trusted_host, data_dir)
        env_file = REPO_ROOT / ".env"
        if not env_file.exists():
            raise RuntimeError(f"{env_file} is required for Docker credentials")
        docker_command = [
            "docker", "run", "-d", "--name", container,
            "--add-host=host.docker.internal:host-gateway",
            "--env-file", str(env_file),
            "-e", f"ELASTIC_URL={container_es_url}",
            "-e", f"ELASTIC_INDEXS={index}",
            "-e", f"ELASTIC_BATCH_SIZE={args.page_size}",
            "-e", f"POLLER_INTERVAL={args.poll_interval}",
            "-e", "ELASTIC_QUERY=*",
            "-e", "VERIFY_SSL=false",
            "-e", "BOOKMARK_PATH=/data",
            "-v", f"{data_dir}:/data",
            args.image,
        ]
        subprocess.run(docker_command, check=True, cwd=REPO_ROOT, text=True)
        log_thread = threading.Thread(
            target=stream_logs, args=(container, log_handle, stop_event), daemon=True
        )
        log_thread.start()
        run_producer(args, host_es_url, index, request_kwargs, stop_event)
    except (OSError, requests.RequestException, subprocess.CalledProcessError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        stop_event.set()
        return 1
    finally:
        stop_event.set()
        subprocess.run(["docker", "stop", container], check=False, capture_output=True, text=True)
        subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True, text=True)
        if not args.keep_index:
            delete_index(host_es_url, index, request_kwargs)
            print(f"deleted index {index}")
        if log_handle:
            log_handle.close()
        shutil.rmtree(data_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
