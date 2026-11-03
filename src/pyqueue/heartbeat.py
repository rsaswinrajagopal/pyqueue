import logging
import os
import threading

import redis

from pyqueue.config import (
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TTL,
    PROCESSING_PREFIX,
)


def heartbeat_key(worker_id: str) -> str:
    return f"worker:{worker_id}:heartbeat"


def processing_key(worker_id: str) -> str:
    return f"{PROCESSING_PREFIX}{worker_id}"


def instance_id(name: str, pid: int | None = None) -> str:
    return f"{name}:{os.getpid() if pid is None else pid}"


class Heartbeat:
    def __init__(
        self,
        worker_id: str,
        client: redis.Redis,
        *,
        interval: float = HEARTBEAT_INTERVAL,
        ttl: int = HEARTBEAT_TTL,
    ) -> None:
        self.worker_id = worker_id
        self._client = client
        self._interval = interval
        self._ttl = ttl
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _beat(self) -> None:
        self._client.set(heartbeat_key(self.worker_id), "ok", ex=self._ttl)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._beat()
            except Exception as exc:  # one failure costs one beat
                logging.warning(f"Heartbeat failed for {self.worker_id}: {exc}")
            self._stop.wait(self._interval)

    def start(self) -> None:
        self._beat()  # lease exists before the worker touches the queue
        self._thread = threading.Thread(
            target=self._run, name=f"heartbeat-{self.worker_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        try:
            self._client.delete(heartbeat_key(self.worker_id))
        except Exception as exc:
            logging.debug(f"Heartbeat deletion failed for {self.worker_id} with {exc}")
