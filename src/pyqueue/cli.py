"""Command line entry point for the PyQueue engine."""

import argparse
import logging
import sys
from importlib.metadata import PackageNotFoundError, version

from pyqueue.log import DEFAULT_LEVEL, configure
from pyqueue.master import MasterProcess
from pyqueue.task import load_task_modules, task_modules_from_env

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

_BANNER = r"""
  ____         ___
 |  _ \ _   _ / _ \ _   _  ___ _   _  ___
 | |_) | | | | | | | | | |/ _ \ | | |/ _ \
 |  __/| |_| | |_| | |_| |  __/ |_| |  __/
 |_|    \__, |\__\_\\__,_|\___|\__,_|\___|
        |___/
 PyQueue Task Engine v{version} | Pure Python & Redis
"""


def _version() -> str:
    try:
        return version("pyqueue")
    except PackageNotFoundError:
        return "0.0.0"


def _print_banner() -> None:
    if not sys.stdout.isatty():
        return
    print(_BANNER.format(version=_version()))


def _run_worker(args: argparse.Namespace) -> int:
    task_modules = args.task_modules or task_modules_from_env()
    if not task_modules:
        logging.warning("No task modules given; every task will be dead-lettered")
    try:
        load_task_modules(task_modules)
    except ImportError:
        return 2
    _print_banner()
    MasterProcess(concurrency=args.concurrency, task_modules=task_modules).run()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pyqueue", description="PyQueue engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser(
        "worker", help="run supervisor and worker pool"
    )
    worker_parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=None,
        help="worker processes (default: cpu count)",
    )
    worker_parser.add_argument(
        "--import",
        dest="task_modules",
        action="append",
        metavar="MODULE",
        help="module holding @task functions; repeatable",
    )
    worker_parser.add_argument(
        "--log-level", default=DEFAULT_LEVEL, type=str.upper, choices=LOG_LEVELS
    )
    worker_parser.set_defaults(handler=_run_worker)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure(args.log_level)
    return args.handler(args)
