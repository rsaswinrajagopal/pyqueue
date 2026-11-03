"""Demo tasks for the end-to-end walkthrough."""

import asyncio

from pyqueue.broker import default_broker
from pyqueue.task import task

COUNTER_PREFIX = "pyqueue:demo:flaky:"


@task
def add(a: float, b: float) -> float:
    return a + b


@task
async def fetch_data(label: str, seconds: float = 1.0) -> str:
    await asyncio.sleep(seconds)
    return f"{label} fetched"


@task
def flaky(run_id: str, succeed_on: int = 3) -> str:
    client = default_broker.client
    key = f"{COUNTER_PREFIX}{run_id}"
    attempt = client.incr(key)
    if attempt < succeed_on:
        raise RuntimeError(f"flaky attempt {attempt} of {succeed_on} failed")
    client.delete(key)
    return f"flaky succeeded on attempt {attempt}"


@task
def poison(run_id: str) -> None:
    raise ZeroDivisionError(f"poison pill {run_id} always fails")
