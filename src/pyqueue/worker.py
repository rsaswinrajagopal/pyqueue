import asyncio
import inspect
import logging

from pyqueue.broker import RedisBroker
from pyqueue.heartbeat import Heartbeat, instance_id
from pyqueue.log import configure
from pyqueue.task import REGISTRY, load_task_modules, task_modules_from_env


def worker(
    worker_id: str, shutdown_event, task_modules: list[str] | None = None
) -> None:
    configure()
    load_task_modules(
        task_modules if task_modules is not None else task_modules_from_env()
    )
    logging.info(f"Loaded {len(REGISTRY)} task(s) from registry")
    broker = RedisBroker()
    ident = instance_id(worker_id)
    heartbeat = Heartbeat(ident, broker.client)
    heartbeat.start()
    try:
        while not shutdown_event.is_set():
            try:
                task_to_run = broker.dequeue(ident)
                if task_to_run is None:
                    continue
                task_message, raw_task = task_to_run
                logging.info(
                    f"{ident} running {task_message.task_name} {task_message.task_id}"
                )
                task_function = REGISTRY.get(task_message.task_name)
                if task_function is None:
                    logging.error(
                        f"Task {task_message.task_name} not found in registry!"
                    )
                    broker.dead_letter(raw_task, ident)
                    continue
                try:
                    if inspect.iscoroutinefunction(task_function):
                        asyncio.run(
                            async_run_task(
                                task_function, task_message.args, task_message.kwargs
                            )
                        )
                    else:
                        run_task(task_function, task_message.args, task_message.kwargs)
                    broker.ack(raw_task, ident)
                    logging.info(f"{ident} finished {task_message.task_id}")
                except Exception as error:
                    logging.error(
                        f"Exception occurred during function execution {error}"
                    )
                    task_message.retries += 1
                    delay_seconds = 2**task_message.retries
                    if task_message.retries <= task_message.max_retries:
                        broker.retry(task_message, raw_task, delay_seconds, ident)
                    else:
                        broker.dead_letter(raw_task, ident)
            except Exception as exc:
                logging.error(f"Exception occurred during function execution {exc}")
    except KeyboardInterrupt:
        logging.info(f"{ident} interrupted, stopping")
    finally:
        heartbeat.stop()


def run_task(func, args, kwargs):
    return func(*args, **kwargs)


async def async_run_task(func, args, kwargs):
    return await func(*args, **kwargs)
