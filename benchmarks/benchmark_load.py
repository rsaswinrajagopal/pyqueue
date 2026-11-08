"""Load benchmark: N no-op tasks, measured end to end."""

import argparse
import multiprocessing as mp
import time

from pyqueue.broker import RedisBroker
from pyqueue.config import PENDING_KEY, PROCESSING_PREFIX
from pyqueue.message import TaskMessage
from pyqueue.worker import worker

TASK_NAME = "pyqueue.demo.add"


def _purge(client) -> None:
    client.delete(PENDING_KEY)
    for key in client.scan_iter(f"{PROCESSING_PREFIX}*"):
        client.delete(key)


def _wait_for_workers(client, expected: int, deadline: float = 10.0) -> None:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if len(list(client.scan_iter("worker:*"))) >= expected:
            return
        time.sleep(0.1)
    raise SystemExit("workers did not come online")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", type=int, default=10_000)
    parser.add_argument("-c", "--concurrency", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    client = RedisBroker().client
    _purge(client)

    ctx = mp.get_context()
    shutdown = ctx.Event()
    workers = [
        ctx.Process(
            target=worker,
            args=(f"Bench-{i + 1}", shutdown, ["pyqueue.demo"]),
        )
        for i in range(args.concurrency)
    ]
    for proc in workers:
        proc.start()
    _wait_for_workers(client, args.concurrency)

    payloads = [
        TaskMessage(task_name=TASK_NAME, args=[i, i]).to_bytes()
        for i in range(args.count)
    ]

    start = time.perf_counter()
    with client.pipeline(transaction=False) as pipe:
        for index, payload in enumerate(payloads, start=1):
            pipe.lpush(PENDING_KEY, payload)
            if index % 500 == 0:
                pipe.execute()
        pipe.execute()
    enqueue_seconds = time.perf_counter() - start

    end = time.monotonic() + args.timeout
    while time.monotonic() < end:
        inflight = sum(
            client.llen(key) for key in client.scan_iter(f"{PROCESSING_PREFIX}*")
        )
        if not client.llen(PENDING_KEY) and not inflight:
            break
        time.sleep(0.05)
    else:
        raise SystemExit("drain timed out")

    elapsed = time.perf_counter() - start
    shutdown.set()
    for proc in workers:
        proc.join(timeout=5)
        if proc.is_alive():
            proc.terminate()

    print(f"tasks        : {args.count}")
    print(f"workers      : {args.concurrency}")
    print(
        f"enqueue      : {enqueue_seconds:.2f}s ({args.count / enqueue_seconds:,.0f}/s)"
    )
    print(f"end to end   : {elapsed:.2f}s ({args.count / elapsed:,.0f} tasks/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
