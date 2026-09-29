"""Direct Broker Inspection (Redis LLEN & RabbitMQ/AMQP queue counts)."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

from kombu import Connection

logger = logging.getLogger(__name__)


class BrokerInspector:
    """Inspects pending queue message depths directly from the broker."""

    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        self.parsed = urlparse(broker_url)
        self.scheme = self.parsed.scheme.lower() if self.parsed.scheme else ""

    def is_redis(self) -> bool:
        return self.scheme.startswith("redis")

    def is_amqp(self) -> bool:
        return self.scheme.startswith("amqp") or self.scheme.startswith("pyamqp")

    def get_queue_depth(self, queue_name: str) -> Optional[int]:
        """Fetch pending message count for a single queue."""
        res = self.get_queues_depths([queue_name])
        return res.get(queue_name)

    def get_queues_depths(self, queue_names: List[str]) -> Dict[str, Optional[int]]:
        """Fetch pending message counts for multiple queues."""
        if not queue_names:
            queue_names = ["celery"]  # Default Celery queue name

        depths: Dict[str, Optional[int]] = {q: None for q in queue_names}

        if self.is_redis():
            try:
                import redis

                r = redis.from_url(self.broker_url, socket_timeout=2.0)
                pipe = r.pipeline()
                for q in queue_names:
                    pipe.llen(q)
                results = pipe.execute()
                for q, count in zip(queue_names, results):
                    depths[q] = int(count) if count is not None else 0
                return depths
            except Exception as e:
                logger.debug(f"Redis queue depth inspection error: {e}")
                return depths

        if self.is_amqp():
            try:
                with Connection(self.broker_url, connect_timeout=2.0) as conn:
                    with conn.channel() as channel:
                        for q in queue_names:
                            try:
                                # passive=True inspects without modifying
                                q_obj = channel.queue_declare(queue=q, passive=True)
                                # message_count is the 2nd return element in AMQP queue_declare_ok
                                message_count = getattr(q_obj, "message_count", None)
                                if message_count is None and isinstance(q_obj, (tuple, list)) and len(q_obj) > 1:
                                    message_count = q_obj[1]
                                depths[q] = int(message_count) if message_count is not None else 0
                            except Exception:
                                depths[q] = None
                return depths
            except Exception as e:
                logger.debug(f"AMQP queue depth inspection error: {e}")
                return depths

        # Generic kombu fallback
        try:
            with Connection(self.broker_url, connect_timeout=2.0) as conn:
                with conn.channel() as channel:
                    for q in queue_names:
                        try:
                            q_obj = channel.queue_declare(queue=q, passive=True)
                            msg_cnt = getattr(q_obj, "message_count", 0)
                            depths[q] = int(msg_cnt)
                        except Exception:
                            depths[q] = None
        except Exception:
            pass

        return depths

    def discover_queues(self) -> List[str]:
        """Attempt to discover active queue names in the broker if supported."""
        discovered = set()
        if self.is_redis():
            try:
                import redis

                r = redis.from_url(self.broker_url, socket_timeout=2.0)
                # Scan for standard Celery list keys
                for key in r.scan_iter(match="*", count=100):
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    # Ignore Celery internal event/unack/set keys
                    if (
                        key_str.startswith("_kombu")
                        or "celery-task-meta" in key_str
                        or key_str.startswith("celery.pidbox")
                    ):
                        continue
                    key_type = r.type(key)
                    type_str = key_type.decode("utf-8") if isinstance(key_type, bytes) else str(key_type)
                    if type_str == "list":
                        discovered.add(key_str)
            except Exception:
                pass
        return list(discovered) or ["celery"]
