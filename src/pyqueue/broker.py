import logging
import time
from typing import cast

import redis

from pyqueue.config import (
    DEAD_LETTER_KEY,
    DELAYED_KEY,
    PENDING_KEY,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
)
from pyqueue.heartbeat import processing_key
from pyqueue.message import TaskMessage


class RedisBroker:
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB):
        self.client = redis.Redis(host=host, port=port, db=db)

    def enqueue(self, message) -> None:
        self.client.lpush(PENDING_KEY, message.to_bytes())

    def dequeue(self, ident) -> tuple[TaskMessage, bytes] | None:
        move_pending = self.client.blmove(
            PENDING_KEY, processing_key(ident), 1, "RIGHT", "LEFT"
        )
        if not isinstance(move_pending, bytes):
            return None
        else:
            message = TaskMessage.from_bytes(move_pending)
            return (message, move_pending)

    def ack(self, raw_task, ident) -> int:
        process_removed = self.client.lrem(processing_key(ident), 1, raw_task)
        return process_removed

    def retry(self, task: TaskMessage, raw_task, delay_seconds, ident):
        try:
            score = time.time() + delay_seconds
            new_task = task.to_bytes()
            self.client.zadd(DELAYED_KEY, {new_task: score})
            self.client.lrem(processing_key(ident), 1, raw_task)
        except Exception as error:
            logging.error(f"Retry failed with {error}")

    def dead_letter(self, raw_task, ident):
        self.client.lpush(DEAD_LETTER_KEY, raw_task)
        self.client.lrem(processing_key(ident), 1, raw_task)

    def promote_delayed_tasks(self):
        now = time.time()
        with self.client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(DELAYED_KEY)
                    tasks = cast(list[bytes], pipe.zrangebyscore(DELAYED_KEY, 0, now))
                    if not tasks:
                        pipe.unwatch()
                        return 0
                    pipe.multi()

                    pipe.lpush(PENDING_KEY, *tasks)
                    pipe.zrem(DELAYED_KEY, *tasks)

                    pipe.execute()
                    return len(tasks)
                except redis.WatchError:
                    continue


default_broker = RedisBroker()
