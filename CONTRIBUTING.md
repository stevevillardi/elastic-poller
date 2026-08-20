# Contributing to edwin-elastic-poller

This guide covers development setup, testing, and implementation details for maintainers.

## Repository layout

| Path | Purpose |
|------|---------|
| `edwin_elastic_poller/` | Python package: config, bookmark, ES client, delivery, poll loop (`python -m edwin_elastic_poller`) |
| `edwin_elastic_poller/config.py` | Environment loading, `EDWIN_*` / `DEXDA_*` aliases, logging bootstrap |
| `edwin_elastic_poller/storage_paths.py` | Bookmark and dedupe file path resolution |
| `edwin_elastic_poller/bookmark.py` | Bookmark read/write |
| `edwin_elastic_poller/elasticsearch.py` | ES transport, PIT, query builder |
| `edwin_elastic_poller/delivery.py` | CEF event creation and Edwin delivery |
| `edwin_elastic_poller/poller.py` | `poll_cycle`, operational summaries |
| `edwin_elastic_poller/sdk/common_event.py` | Maps raw records to Common Event Format (CEF) |
| `edwin_elastic_poller/sdk/edwin_request.py` | HTTP client for Edwin OAuth and event ingestion |
| `edwin_elastic_poller/observability/lm_logs.py` | Optional LogicMonitor Logs ingestion handler |
| `edwin_elastic_poller/mappings/elastic_event_mappings.yaml` | JSONPath mappings from ES documents to CEF fields (override with `EVENT_MAPPING_FILE`) |
| `tests/test_edwin_elastic_poller.py` | Unit tests for bookmark, query, pagination, and mapping |
| `tests/test_lm_logs.py` | Unit tests for LM Logs handler and sanitization |
| `tests/test_env_config.py` | Unit tests for Edwin credential env aliases |
| `tests/test_integration_elasticsearch.py` | Integration tests against a real Elasticsearch (skipped unless `ES_TEST_URL` is set) |
| `tests/test_integration_multipoll.py` | Multi-poll integration tests (seed waves between poll cycles) |
| `tests/test_live_delivery.py` | Live Edwin delivery tests (requires `ES_LIVE_DELIVERY=1` and `EDWIN_*` or `DEXDA_*`) |
| `tests/es_test_support.py` | Shared Elasticsearch seeding helpers for integration and E2E runs |
| `scripts/local_e2e.py` | Interactive local multi-poll runner |
| `pyproject.toml` | Package metadata, runtime dependencies, and console entry point |

## Installing for development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Runtime dependencies are pinned in `pyproject.toml`. Docker and `pip install edwin-elastic-poller` both install from that same metadata.

## Releasing to PyPI

Publishing is automated by [`.github/workflows/publish.yml`](.github/workflows/publish.yml) when a GitHub release is published.

1. Bump `version` in `pyproject.toml`.
2. Create a git tag matching that version (`v0.1.0` for version `0.1.0`).
3. Publish a GitHub release from the tag.

| Release type | Destination |
|--------------|-------------|
| **Pre-release** | [TestPyPI](https://test.pypi.org/project/edwin-elastic-poller/) |
| **Full release** | [PyPI](https://pypi.org/project/edwin-elastic-poller/) |

The workflow verifies that the release tag matches `pyproject.toml` before uploading.

To publish manually from the Actions tab, run **Publish to PyPI** with `workflow_dispatch` and choose `testpypi` or `pypi`.

Publishing uses [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/) (OpenID Connect) via `pypa/gh-action-pypi-publish`. Configure trusted publishers on TestPyPI and PyPI for this repository, then create GitHub environments named `testpypi` and `pypi` (no API token secrets required).

## Supported versions

| Component | Supported | Notes |
|-----------|-----------|-------|
| **Elasticsearch** | **8.x / 9.x** (recommended), **7.12+** | Each poll cycle opens a point-in-time (`_pit`, ES 7.10+) and paginates with `search_after`, using `_shard_doc` as the sort tie-breaker. `_shard_doc` was added in ES 7.12 and is only valid inside a PIT, so ES 7.11 and older are not supported. |
| **Kibana** | **7.x / 8.x / 9.x** | Poller reads the Kibana alerting **event log** (e.g. `.kibana-event-log-ds`), not raw log/filebeat indices |

CI runs the integration suite against **8.11.4, 8.19.20 and 9.5.1**. 8.11.4 is pinned deliberately: Elastic relaxed the `_shard_doc` validation during the 8.x line, so 8.11.4 rejects `_shard_doc` outside a point-in-time with HTTP 400 while 8.19.20 and 9.5.1 accept it. Testing only the latest releases would miss that class of regression.

## Bookmark and pagination internals

- Bookmark is stored as **epoch milliseconds** in `{BOOKMARK_PATH}/{EDWIN_ORG or DEXDA_ORG}.elastic.bookmark`
- Queries use an **exclusive** lower bound (`gt` + `epoch_millis`) so the last sent event is never re-fetched
- **`search_after`** pagination drains backlogs larger than `ELASTIC_BATCH_SIZE` within a single cycle
- Each cycle paginates inside a **point-in-time snapshot**, so pages cannot skip or duplicate documents when many events share the same millisecond, or when the index refreshes mid-cycle. The PIT is always released, including when delivery fails.
- If no bookmark file exists, polling starts at **now − 2 hours**
- Bookmark is **not** advanced if Edwin delivery fails
- Bookmark is persisted after each successfully delivered page
- An Elasticsearch error mid-cycle aborts that cycle, keeps the bookmark already committed, and retries on the next interval

### Known limitation

> The bookmark has millisecond granularity, and it advances to the last delivered hit's `@timestamp` after every page. If a page boundary falls inside a group of documents sharing one millisecond and the cycle then aborts (delivery failure, PIT expiry, restart), the next cycle's `gt` bound skips the remainder of that group. The point-in-time makes pagination correct *within* a cycle; it does not close this gap *between* cycles.

**Mitigations:**

- Increase `ELASTIC_PIT_KEEP_ALIVE` if cycles are slow (reduces PIT expiry mid-cycle)
- Ensure reliable Edwin delivery (failures mid-cycle are the most common trigger)
- Monitor delivery completeness if you expect very high same-millisecond event volume

**Future fix options** (not implemented):

- Defer bookmark persistence to end of cycle (accepts possible duplicates on retry; Edwin dedupes on `event_id`)
- Compound bookmark with resume cursor and dedupe logic

## LM Logs handler

The optional `lm_logs.py` module ships operational logs to the [LogicMonitor Logs ingestion API](https://www.logicmonitor.com/support/lm-logs/sending-logs-to-ingestion-api).

### Architecture

- `configure_logging()` sets up a stderr `StreamHandler` and an optional `LmLogsHandler`
- `LmLogsHandler` POSTs to `https://{account}.logicmonitor.com/rest/log/ingest` with `Authorization: Bearer {token}`
- Level filtering is applied by the logger (`record.levelno >= handler.level`) before the handler is invoked
- When `DEBUG=false`, stderr shows INFO+ and the LM handler ships INFO+ operational logs (poll summaries, errors, startup). Set `DEBUG=true` or `LM_LOGS_VERBOSE=true` to also ship DEBUG detail to LM Logs.
- Handler failures are swallowed and reported to stderr only — they never interrupt the poll loop
- `common_event` and `edwin_request` live under `edwin_elastic_poller/sdk/` and log recoverable mapping misses and per-batch HTTP detail at DEBUG. Real failures remain at ERROR.

### Sanitization rules

| Field | Treatment |
|-------|-----------|
| `EDWIN_TOKEN`, `EDWIN_ID` | Omit |
| `ELASTIC_PASS`, `ELASTIC_TOKEN` | Omit |
| `LM_LOGS_BEARER_TOKEN` | Omit |
| `ELASTIC_URL` with embedded credentials | Strip to `host:port` |
| Event payloads / CEF content | Omit |
| Bookmark file contents | Log path and ms timestamp only |

Structured metadata is attached via `log_with_context()` using the `lm_context` extra field on log records.

## Local Elasticsearch testing

Start a single-node instance:

```bash
docker run -d --name edwin-elastic-poller-es \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e xpack.security.http.ssl.enabled=false \
  -e cluster.routing.allocation.disk.threshold_enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  docker.elastic.co/elasticsearch/elasticsearch:8.19.20
```

### Multi-poll end-to-end (mock delivery)

Integration tests seed documents in waves and run multiple `poll_cycle` calls to exercise bookmark advancement and pagination together:

```bash
ES_TEST_URL=http://localhost:9200 ES_REQUIRE_INTEGRATION=1 \
  python -m unittest tests.test_integration_multipoll -v
```

For interactive local runs without unittest:

```bash
python scripts/local_e2e.py --es-url http://localhost:9200 --batches 4 --docs-per-batch 20
```

Each wave indexes a batch of documents, runs one poll cycle, and verifies the cumulative delivery set. With the default page size of 5 and 20 documents per batch, each wave paginates across multiple Elasticsearch pages.

### Live delivery (Edwin / LM Logs)

Live tests send a small number of real events to Edwin. They are skipped unless credentials and an explicit opt-in flag are set:

```bash
ES_TEST_URL=http://localhost:9200 \
ES_LIVE_DELIVERY=1 \
EDWIN_ORG=... EDWIN_ID=... EDWIN_TOKEN=... \
  python -m unittest tests.test_live_delivery -v
```

(`DEXDA_ORG`, `DEXDA_ID`, and `DEXDA_TOKEN` work as aliases for the `EDWIN_*` names.)

Optional LM Logs shipping during the live run:

```bash
LM_LOGS_ENABLED=true LM_LOGS_BEARER_TOKEN=... \
  python -m unittest test_live_delivery.py -v
```

Or use the local E2E script with live delivery:

```bash
ES_LIVE_DELIVERY=1 python scripts/local_e2e.py --es-url http://localhost:9200 --live
```

## Running tests

Run from the **repository root** or install the package with `pip install -e .`.

```bash
# All tests. Integration modules skip themselves when ES_TEST_URL is unset.
python -m unittest discover -s tests -t . -p "test_*.py" -v

# Integration tests against a running Elasticsearch (see above).
ES_TEST_URL=http://localhost:9200 ES_REQUIRE_INTEGRATION=1 \
  python -m unittest discover -s tests -t . -p "test_integration_*.py" -v
```

`ES_REQUIRE_INTEGRATION=1` turns a missing `ES_TEST_URL` into a hard failure, so a misconfigured run cannot pass by skipping every test.

CI (see `.github/workflows/test.yml`) runs:

- Unit tests on every push and pull request (`pip install -e .`)
- Integration tests matrixed over Elasticsearch 8.11.4, 8.19.20 and 9.5.1 (including multi-poll tests)
- Kibana event-log verification against Elasticsearch/Kibana 8.19.20
- A **live delivery** job when repository secrets `EDWIN_*` or `DEXDA_*` credentials are configured
- PyPI/TestPyPI publishing on GitHub release (see [Releasing to PyPI](#releasing-to-pypi))
