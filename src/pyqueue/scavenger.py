import logging
import threading

from pyqueue.broker import RedisBroker
from pyqueue.config import (
    PENDING_KEY,
    PROCESSING_PREFIX,
    SCAVENGER_INTERVAL,
    TICK_INTERVAL,
)
from pyqueue.heartbeat import heartbeat_key, processing_key


class Scavenger:
    def __init__(
        self,
        broker: RedisBroker,
        tick: float = TICK_INTERVAL,
        sweep: float = SCAVENGER_INTERVAL,
    ) -> None:
        self.broker = broker
        self.tick = tick
        self.sweep_every = max(1, round(sweep / tick))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="scavenger", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.tick * 2)

    def _run(self) -> None:
        tick = 0
        while not self._stop.wait(self.tick):
            tick += 1
            try:
                promoted = self.broker.promote_delayed_tasks()
                if promoted:
                    logging.info(f"Promoted {promoted} delayed task(s)")
                if tick % self.sweep_every == 0:
                    try:
                        self._sweep()
                    except Exception:
                        logging.warning("Scavenger sweep got failed")
                        continue
            except Exception:
                logging.exception("scavenger tick failed")

    def _sweep(self) -> None:
        for key in self.broker.client.scan_iter(f"{PROCESSING_PREFIX}*", count=500):
            ident = key.decode().removeprefix(PROCESSING_PREFIX)
            try:
                if self.broker.client.exists(heartbeat_key(ident)):
                    continue
                self._drain(ident)
            except Exception:
                logging.exception(f"reclaim failed for {ident!r}")

    def _drain(self, ident: str) -> int:
        moved = 0
        while (
            self.broker.client.lmove(
                processing_key(ident), PENDING_KEY, "RIGHT", "LEFT"
            )
            is not None
        ):
            moved += 1
        if moved:
            logging.warning(f"Reclaimed {moved} task(s) from dead worker {ident}")
        return moved
