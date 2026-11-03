"""Chaos test: SIGKILL a worker mid-task and prove the Scavenger reclaims it."""

import multiprocessing as mp
import time

import pytest
import redis

from pyqueue.broker import RedisBroker
from pyqueue.config import (
    PENDING_KEY,
    PROCESSING_PREFIX,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
)
from pyqueue.heartbeat import instance_id
from pyqueue.message import TaskMessage
from pyqueue.scavenger import Scavenger
from pyqueue.tasks import slow
from pyqueue.worker import worker

WORKER_NAME = "ChaosWorker"
PICKUP_DEADLINE = 10.0
RECLAIM_DEADLINE = 35.0


@pytest.fixture()
def broker():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis is not running")
    yield RedisBroker()
    client.delete(PENDING_KEY)
    for key in client.scan_iter(f"{PROCESSING_PREFIX}*"):
        client.delete(key)


def _pending_ids(client) -> set:
    return {
        TaskMessage.from_bytes(raw).task_id for raw in client.lrange(PENDING_KEY, 0, -1)
    }


def test_scavenger_reclaims_task_from_killed_worker(broker):
    shutdown_event = mp.Event()
    proc = mp.Process(
        target=worker, args=(WORKER_NAME, shutdown_event, ["pyqueue.tasks"])
    )
    proc.start()
    ident = instance_id(WORKER_NAME, proc.pid)
    scavenger = Scavenger(broker, tick=0.2, sweep=1.0)
    scavenger.start()

    try:
        task_id = slow.delay(30)

        deadline = time.monotonic() + PICKUP_DEADLINE
        while time.monotonic() < deadline:
            if broker.client.llen(f"{PROCESSING_PREFIX}{ident}"):
                break
            time.sleep(0.1)
        else:
            pytest.fail("worker never picked up the task")

        proc.kill()
        proc.join(timeout=5)
        assert not proc.is_alive()

        deadline = time.monotonic() + RECLAIM_DEADLINE
        while time.monotonic() < deadline:
            if task_id in _pending_ids(broker.client):
                return
            time.sleep(0.5)
        pytest.fail("scavenger did not reclaim the orphaned task")
    finally:
        scavenger.stop()
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)
        shutdown_event.set()
