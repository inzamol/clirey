"""Direct Broker Inspection (Redis LLEN & RabbitMQ/AMQP queue counts)."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

from kombu import Connection

logger = logging.getLogger(__name__)


class BrokerInspector:
    """Inspects pending queue message depths directly from the broker."""

    def __init__(self, broker_url: str, key_prefix: Optional[str] = None):
        self.broker_url = broker_url
        self.key_prefix = key_prefix or ""
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
                    redis_key = f"{self.key_prefix}{q}"
                    pipe.llen(redis_key)
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

    @classmethod
    def auto_detect_key_prefix(cls, broker_url: str) -> Optional[str]:
        """Attempt to auto-detect Redis global_keyprefix by inspecting kombu/celery keys."""
        parsed = urlparse(broker_url)
        scheme = parsed.scheme.lower() if parsed.scheme else ""
        if not scheme.startswith("redis"):
            return None
        try:
            from collections import Counter

            import redis

            r = redis.from_url(broker_url, socket_timeout=1.0, socket_connect_timeout=1.0)
            candidates = []
            scanned = 0
            for key in r.scan_iter(count=50):
                key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                if "_kombu.binding." in key_str:
                    candidates.append(key_str.split("_kombu.binding.", 1)[0])
                elif "celery.pidbox" in key_str:
                    candidates.append(key_str.split("celery.pidbox", 1)[0])
                elif "celeryev" in key_str:
                    candidates.append(key_str.split("celeryev", 1)[0])

                scanned += 1
                if scanned >= 100:
                    break

            if candidates:
                counts = Counter(c for c in candidates if c)
                if counts:
                    most_common, _ = counts.most_common(1)[0]
                    return most_common
        except Exception as e:
            logger.debug(f"Auto-detect key prefix error: {e}")
        return None

    def discover_queues(self) -> List[str]:
        """Attempt to discover active queue names in the broker if supported."""
        discovered = set()
        if self.is_redis():
            try:
                import redis

                r = redis.from_url(self.broker_url, socket_timeout=1.0, socket_connect_timeout=1.0)
                scan_pattern = f"{self.key_prefix}*" if self.key_prefix else "*"
                scanned = 0
                keys_to_check = []
                for key in r.scan_iter(match=scan_pattern, count=100):
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    # Strip key prefix if present to check standard Celery system keys
                    base_key = (
                        key_str[len(self.key_prefix) :]
                        if (self.key_prefix and key_str.startswith(self.key_prefix))
                        else key_str
                    )
                    if (
                        base_key.startswith("_kombu")
                        or "celery-task-meta" in base_key
                        or "celery.pidbox" in base_key
                        or base_key.startswith("celeryev")
                        or ".reply." in base_key
                    ):
                        continue
                    keys_to_check.append(key_str)
                    scanned += 1
                    if scanned >= 100:
                        break

                if keys_to_check:
                    pipe = r.pipeline()
                    for k in keys_to_check:
                        pipe.type(k)
                    types = pipe.execute()
                    for k, t in zip(keys_to_check, types):
                        t_str = t.decode("utf-8") if isinstance(t, bytes) else str(t)
                        if t_str == "list":
                            # Strip key prefix if present for clean display
                            clean_name = (
                                k[len(self.key_prefix) :] if self.key_prefix and k.startswith(self.key_prefix) else k
                            )
                            discovered.add(clean_name)
            except Exception:
                pass
        return list(discovered) or ["celery"]
