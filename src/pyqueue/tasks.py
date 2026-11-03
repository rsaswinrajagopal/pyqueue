import time

from pyqueue.task import task


@task
def slow(seconds: float = 30.0) -> str:
    time.sleep(seconds)
    return "done"
