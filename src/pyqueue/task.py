import importlib
import logging
import os

from pyqueue.broker import default_broker
from pyqueue.message import TaskMessage

# _REGISTRY: dict[str, str] = {}
REGISTRY = {}
REDIS = default_broker
TASKS_ENV = "PYQUEUE_TASKS"


def load_task_modules(paths: list[str]) -> None:
    """Import task modules so their @task functions land in REGISTRY."""
    for path in paths:
        name = path.strip()
        if not name:
            continue
        try:
            importlib.import_module(name)
        except ImportError as exc:
            logging.error(f"Could not import task module {name!r}: {exc}")
            raise


def task_modules_from_env() -> list[str]:
    raw = os.environ.get(TASKS_ENV, "")
    return [name.strip() for name in raw.split(",") if name.strip()]


def task(func):
    def delay(*args, **kwargs):
        task_message = TaskMessage(
            task_name=f"{func.__module__}.{func.__name__}",
            args=list(args),
            kwargs=dict(kwargs),
        )
        REDIS.enqueue(task_message)
        return task_message.task_id

    func.delay = delay
    REGISTRY[f"{func.__module__}.{func.__name__}"] = func
    return func
