"""Entry point for running the poller as a module."""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone

from edwin_elastic_poller import bookmark, config, elasticsearch, poller
from edwin_elastic_poller import storage_paths
from edwin_elastic_poller.observability import lm_logs


def main() -> None:
    try:
        config.bootstrap()
        config.validate_config()
        poller.log_startup()
        config.logger.info(
            "Edwin org: %s | Elasticsearch: %s | Bookmark file: %s",
            config.EDWIN_ORG,
            lm_logs.sanitize_elastic_url(config.ELASTIC_URL),
            storage_paths.bookmark_file(),
        )

        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        default_bookmark = int(two_hours_ago.timestamp() * 1000)
        bookmark_ms = default_bookmark
        bookmark_loaded = False

        stored_bookmark = bookmark.get_bookmark()
        if stored_bookmark > 0:
            bookmark_loaded = True
            bookmark_ms = stored_bookmark

        watermark = bookmark_ms
        config.logger.info(
            "Starting from bookmark %s (%s)",
            bookmark_ms,
            elasticsearch.epoch_ms_to_zulu(bookmark_ms),
        )

        while True:
            bookmark_ms = poller.poll_cycle(bookmark_ms, watermark, bookmark_loaded)
            config.logger.info(
                "Sleeping %s seconds until next poll cycle", config.PAUSE_INTERVAL
            )
            time.sleep(int(config.PAUSE_INTERVAL))
    except config.ConfigurationError as error:
        config.logger.error("Configuration validation failed: %s", error)
        sys.exit(config.ERROR_CODE_VALIDATION_FAILED)
    except KeyboardInterrupt:
        config.logger.info("Shutdown requested")
        sys.exit(config.OK)
    except Exception:
        config.logger.exception("Unhandled edwin-elastic-poller failure")
        sys.exit(config.ERROR_CODE_UNEXPECTED)


if __name__ == "__main__":
    main()
