import logging
import os
import signal
import time
from multiprocessing import Event, Process

from pyqueue.broker import RedisBroker
from pyqueue.config import DEFAULT_WORKER_COUNT
from pyqueue.scavenger import Scavenger
from pyqueue.worker import worker


class MasterProcess:
    def __init__(
        self,
        concurrency: int | None = None,
        task_modules: list[str] | None = None,
    ) -> None:
        self.concurrency: int = concurrency or os.cpu_count() or DEFAULT_WORKER_COUNT
        self.task_modules: list[str] = task_modules or []
        self.workers: list[Process] = []
        self.shutdown_event = Event()
        self.broker = None
        self.scavenger = None

    def _setup_signals(self) -> None:
        """Trap OS termination signals for a graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, frame) -> None:
        signame = signal.Signals(signum).name
        logging.info(f"Received {signame}. Initiating graceful shutdown...")
        self.shutdown_event.set()

    def _spawn_worker(self, worker_name: str) -> Process:
        """Helper to instantiate and launch an individual child worker."""
        p = Process(
            target=worker,
            name=worker_name,
            args=(
                worker_name,
                self.shutdown_event,
                self.task_modules,
            ),  # Pass the stop flag to the child
        )
        p.start()
        return p

    def start(self) -> None:
        for i in range(self.concurrency):
            worker_name = f"Worker-{i + 1}"
            p = self._spawn_worker(worker_name)
            self.workers.append(p)
            logging.info(f"Spawned {worker_name} (PID: {p.pid})")

    def run(self) -> None:
        """Main lifecycle entrypoint: registers signals, starts pool, and supervises."""
        self._setup_signals()
        self.start()
        self.broker = RedisBroker()
        self.scavenger = Scavenger(self.broker)
        self.scavenger.start()
        try:
            while not self.shutdown_event.is_set():
                time.sleep(1)

                # Iterate over a shallow copy to safely remove/re-add dead processes
                for p in list(self.workers):
                    if not p.is_alive():
                        logging.warning(
                            f"Worker {p.name} (PID: {p.pid}) died with exit code {
                                p.exitcode
                            }"
                        )
                        p.close()
                        self.workers.remove(p)

                        # Only respawn if we are NOT in the middle of shutting down
                        if not self.shutdown_event.is_set():
                            new_worker = self._spawn_worker(p.name)
                            self.workers.append(new_worker)
                            logging.info(
                                f"Respawned replacement for {p.name} (PID: {
                                    new_worker.pid
                                })"
                            )

        except KeyboardInterrupt:
            # Fallback in case signal handler did not catch it cleanly
            self.shutdown_event.set()

        finally:
            self.shutdown()

    def shutdown(self, timeout: float = 5.0) -> None:
        logging.info("Stopping all workers...")
        self.shutdown_event.set()

        # Step 1: Wait politely for workers to finish current tasks
        start_time = time.time()
        for p in self.workers:
            remaining_time = max(0.0, timeout - (time.time() - start_time))
            p.join(timeout=remaining_time)

        # Step 2: Forcefully kill (SIGTERM/SIGKILL) any workers that are still hanging
        for p in self.workers:
            if p.is_alive():
                logging.warning(f"Worker {p.name} timed out. Sending SIGTERM...")
                p.terminate()
                p.join()
            p.close()
        if self.scavenger is not None:
            self.scavenger.stop()

        self.workers.clear()
        logging.info("All workers terminated cleanly. Master process exiting.")
