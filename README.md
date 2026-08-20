# Edwin Event Integration (Elasticsearch → Edwin)

The **Edwin Event Integration** connects your Elasticsearch Kibana alerting event log to **Edwin**. It continuously polls Elasticsearch for new events, maps them to Common Event Format (CEF), and delivers them to Edwin over HTTPS—with durable bookmarking so events are processed once and large backlogs drain safely across poll cycles.

This repository ships the integration as the `edwin-elastic-poller` service (Docker image and Python package). The commands and file names below use that technical name; in documentation and operations, refer to the product as **Edwin Event Integration**.

## What it does

1. Opens a point-in-time (PIT) search against your Elasticsearch event-log index.
2. Fetches documents newer than the stored bookmark, in pages of `ELASTIC_BATCH_SIZE`.
3. Maps each document to CEF using the bundled mapping in `edwin_elastic_poller/mappings/`.
4. Delivers batches to Edwin over HTTPS.
5. Advances the bookmark only after Edwin accepts a batch.
6. Sleeps for `POLLER_INTERVAL` seconds, then repeats.

On first run (no bookmark file, or bookmark is `0`), the integration starts polling at **now − 2 hours**.

## Requirements

| Component | Supported versions |
|-----------|-------------------|
| **Elasticsearch** | 8.x / 9.x recommended; 7.12+ minimum |
| **Kibana** | 7.x / 8.x / 9.x (event log data stream, e.g. `.kibana-event-log-ds`) |
| **Edwin** | OAuth client credentials (`EDWIN_ORG`, `EDWIN_ID`, `EDWIN_TOKEN`) |
| **Python** | 3.12+ (for non-Docker installs) |

The integration reads the **Kibana event log**, not raw log or Beats indices.

## Quick start (pip)

Install the latest release from PyPI:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install edwin-elastic-poller

cp .env.example .env
# edit .env with your credentials

edwin-elastic-poller
# or: python -m edwin_elastic_poller
```

Pre-releases are published to [TestPyPI](https://test.pypi.org/project/edwin-elastic-poller/) first. To try a candidate build:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ edwin-elastic-poller
```

## Quick start (Docker)

### 1. Configure environment

Copy the example file and fill in your values. The file is used only as a
local runtime environment file; it is not copied into the image:

```bash
cp .env.example .env
```

At minimum, set Edwin credentials and Elasticsearch connection details (see [Configuration](#configuration) below).

### 2. Build the image

```bash
docker build -t edwin-elastic-poller .
```

### 3. Run the container

Mount a volume for the bookmark so progress survives restarts:

```bash
docker run -d \
  --name edwin-elastic-poller \
  --restart unless-stopped \
  --env-file .env \
  -v edwin-elastic-poller-data:/data \
  edwin-elastic-poller
```

The default bookmark path inside the container is `/data/{EDWIN_ORG}.elastic.bookmark`.

### 4. Verify it is running

Check container logs for startup and poll-cycle summaries:

```bash
docker logs -f edwin-elastic-poller
```

You should see lines like:

```text
INFO - edwin-elastic-poller started
INFO - Poll cycle finished: status=complete, events_delivered=12, pages_fetched=1, ...
```

## Quick start (development)

For local development from a git checkout:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

cp .env.example .env
# edit .env with your credentials

python -m edwin_elastic_poller
```

Dependencies are declared in `pyproject.toml` only. Run from the repository root or any directory after installing the package.

## Configuration

All settings are loaded from environment variables (or a `.env` file in the working directory).

### Edwin credentials

| Variable | Required | Description |
|----------|----------|-------------|
| `EDWIN_ORG` | Yes | Your Edwin portal prefix (subdomain) |
| `EDWIN_ID` | Yes | OAuth client ID |
| `EDWIN_TOKEN` | Yes | OAuth client secret |

Legacy aliases `DEXDA_ORG`, `DEXDA_ID`, and `DEXDA_TOKEN` are still accepted if `EDWIN_*` is not set.

### Elasticsearch

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ELASTIC_URL` | Yes | — | Elasticsearch base URL (e.g. `https://es.example.com:9200`) |
| `ELASTIC_INDEXS` | Yes | — | Index name, comma-separated list, or wildcard (e.g. `.kibana-event-log-ds`) |
| `ELASTIC_BATCH_SIZE` | No | `500` | Documents per page within a poll cycle |
| `ELASTIC_USER` | No* | — | Basic auth username |
| `ELASTIC_PASS` | No* | — | Basic auth password |
| `ELASTIC_TOKEN` | No* | — | API key (alternative to user/password) |
| `ELASTIC_QUERY` | No | `*` | Lucene `query_string` filter applied in addition to the bookmark range |
| `VERIFY_SSL` | No | `true` | TLS verification for all outbound HTTPS (Elasticsearch, Edwin, LM Logs). Set `false` only in controlled test environments |
| `ELASTIC_VERIFY_SSL` | No | — | Deprecated; use `VERIFY_SSL`. Honored only when `VERIFY_SSL` is unset |
| `EDWIN_VERIFY_SSL` | No | — | Deprecated; use `VERIFY_SSL`. Honored only when `VERIFY_SSL` is unset |
| `ELASTIC_PIT_KEEP_ALIVE` | No | `5m` | Point-in-time lease per poll cycle (extended on each page) |
| `ELASTIC_OVERLAP_MS` | No | `300000` | History reread window for delayed events; delivered documents are deduplicated |
| `DEDUPE_MAX_RECORDS` | No | `250000` | Maximum retained document identities |
| `DEDUPE_MAX_SIZE_MB` | No | `256` | Maximum deduplication database size |

\* Provide either basic auth (`ELASTIC_USER` + `ELASTIC_PASS`) or `ELASTIC_TOKEN`.

**Recommended query filter** to reduce scheduler noise:

```bash
ELASTIC_QUERY=NOT event.action:execute-start
```

### Integration runtime

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POLLER_INTERVAL` | No | `240` | Seconds to sleep between poll cycles |
| `BOOKMARK_PATH` | No | `.` (host) / `/data/` (Docker image) | Directory for the bookmark file |
| `LOG` | No | `true` | Set `false` to disable logging |
| `DEBUG` | No | `false` | Verbose stderr logging and SDK detail |

### Bookmark file

The bookmark is stored as epoch **milliseconds** at:

```text
{BOOKMARK_PATH}/{EDWIN_ORG}.elastic.bookmark
```

Example on a host with `BOOKMARK_PATH=./data` and `EDWIN_ORG=acme`:

```text
./data/acme.elastic.bookmark
```

- Created automatically on first read (initial value `0`).
- Updated after each **successfully delivered** page.
- **Not** advanced if Edwin delivery fails for a batch.
- Writes are atomic; an unreadable bookmark stops startup rather than silently
  resetting progress.

To reprocess historical events, stop the integration, delete or reset the bookmark file, and restart. Setting the file to `0` causes the next run to use the default start time (now − 2 hours).

### Optional: LogicMonitor Logs (LM Logs)

Ship operational logs to LM Logs for monitoring the integration in the LogicMonitor portal.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LM_LOGS_ENABLED` | No | `false` | Enable LM Logs ingestion |
| `LM_LOGS_ACCOUNT` | No | `EDWIN_ORG` | LogicMonitor portal prefix |
| `LM_LOGS_BEARER_TOKEN` | Yes* | — | Bearer token with **Logs Manage** permission |
| `LM_LOGS_RESOURCE_ID` | No | — | LogicMonitor device ID to associate logs with |
| `LM_LOGS_VERBOSE` | No | `false` | Ship DEBUG-level detail to LM Logs |

\* Required when `LM_LOGS_ENABLED=true`.

Create a bearer token under **Settings → Users & Roles** with Logs Manage permission. See [LogicMonitor LM Logs ingestion](https://www.logicmonitor.com/support/lm-logs/sending-logs-to-ingestion-api).

When enabled, each poll cycle emits an operational summary (search for `Poll cycle finished` or `event_type=poll_summary`). Credentials and event payloads are never written to LM Logs.

For permanent Edwin payload rejections, failed payload persistence is disabled
by default. Set `FAILED_PAYLOAD_PATH` to a protected directory only when
payload inspection is required.

## Event mapping

Documents are converted to CEF using the bundled mapping file at `edwin_elastic_poller/mappings/elastic_event_mappings.yaml`. JSONPath expressions map Elasticsearch fields to CEF attributes (CI, severity, event ID, and so on).

To customize mapping for your environment, either:

- Set `EVENT_MAPPING_FILE` to the path of your YAML file (absolute or relative to the process working directory), or
- Edit the bundled file in a development checkout and restart the integration.

Field mappings follow the Common Event schema used by Edwin.

| Variable | Required | Description |
|----------|----------|-------------|
| `EVENT_MAPPING_FILE` | No | Path to a custom mapping YAML; when unset, the bundled default is used |

## How it works

```text
┌─────────────┐     PIT search      ┌────────────────────────┐     CEF batches     ┌───────┐
│ Elasticsearch│ ─────────────────► │ Edwin Event Integration │ ──────────────────► │ Edwin │
│ (event log)  │ ◄── bookmark gt ── │ (edwin-elastic-poller)        │                     └───────┘
└─────────────┘                     └───────────┬────────────┘
                                                │
                                         bookmark file
                                         (last delivered @timestamp)
```

- Each poll cycle uses a **point-in-time snapshot** and `search_after` pagination so pages stay consistent even when many events share the same millisecond.
- A configurable overlap window rereads delayed or out-of-order events, while a local SQLite ledger prevents duplicate Elasticsearch documents from being sent again.
- The deduplication database is pruned every cycle and constrained by both
  record-count and file-size limits. If limits force eviction, duplicate
  redelivery is possible and is reported in the cycle summary.
- Backlogs larger than one batch are drained within a single cycle before sleeping.
- If Edwin rejects a batch, the bookmark stays at the last successful position and the cycle retries on the next interval.
- Edwin deduplicates on `event_id` if a small number of events are redelivered after a failure.

## Operations

### Log levels

| `DEBUG` | What you see |
|---------|----------------|
| `false` | Startup, poll-cycle summaries (`Poll cycle finished: ...`), warnings, and errors |
| `true` | Above plus per-page Elasticsearch and mapping detail |

### Health checks

Monitor for:

- Regular `Poll cycle finished` messages with `status=complete`
- `events_delivered` increasing when new Kibana events are present
- `status=delivery_failed` or `errors=true` — investigate Edwin connectivity or credentials
- `status=pit_expired` — consider increasing `ELASTIC_PIT_KEEP_ALIVE` if cycles are slow

### Upgrades

1. Stop the container or process.
2. Pull or build the new image.
3. Ensure the bookmark volume (`/data` or your `BOOKMARK_PATH`) is preserved.
4. Start the new version with the same environment.

## Troubleshooting

| Symptom | Things to check |
|---------|-----------------|
| No events delivered | `ELASTIC_INDEXS` points at the Kibana event log; `ELASTIC_QUERY` is not too restrictive; bookmark is not ahead of available data |
| Bookmark never advances | Edwin credentials; network egress to `{EDWIN_ORG}.dexda.ai`; container logs for `delivery_failed` |
| `pit_expired` in logs | Slow cycles or large backlogs — increase `ELASTIC_PIT_KEEP_ALIVE` |
| `pit_open_failed` / `Connection refused` to `localhost:9200` | Containerized poller cannot reach ES at `localhost` — use `host.docker.internal`, a Compose service name, or the remote ES hostname (see [Docker quick start](#quick-start-docker)) |
| Empty `./data` after tests | Unit and integration tests use temporary bookmark paths; only `python -m edwin_elastic_poller` or Docker with a mounted volume writes the production bookmark |
| Mapping warnings in DEBUG | Expected fallback behavior when optional JSONPath fields are missing; operational impact is none if events reach Edwin |

## Development

For maintainers — local testing, CI, releases, and implementation details — see [CONTRIBUTING.md](CONTRIBUTING.md).

### Disposable Kibana integration stack

Start Elasticsearch, Kibana, six real `.es-query` Kibana rules, and one test
document with:

```bash
docker compose -f docker-compose.kibana-test.yml up
```

The stack exposes Elasticsearch at `http://localhost:9200` and Kibana at
`http://localhost:5601`. The setup container creates active, non-matching, and
threshold-variation rules, then deletes them when the stack is stopped.
Elasticsearch and Kibana data are disposable because no volumes are configured.

Stop and tear down everything with:

```bash
docker compose -f docker-compose.kibana-test.yml down --volumes --remove-orphans
```

Point a **host-run** poller at the generated event log with
`ELASTIC_INDEXS=.kibana-event-log-ds`, `ELASTIC_QUERY=*`, and
`ELASTIC_URL=http://localhost:9200`.

If the poller runs **in Docker** while this stack is up, use
`ELASTIC_URL=http://host.docker.internal:9200` (Docker Desktop) or attach the
poller to the Compose network with `ELASTIC_URL=http://elasticsearch:9200`.
This stack does not configure Edwin delivery; use a test credential set only when delivery is intentionally
required.

## License

See repository license and SPDX headers in source files.
