"""Poll Elasticsearch for Kibana alerting events and forward them to Edwin."""

from edwin_elastic_poller.poller import poll_cycle

__all__ = ["poll_cycle"]
