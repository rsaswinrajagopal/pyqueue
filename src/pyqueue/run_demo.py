"""Producer for the end-to-end demo. Run from the repository root."""

import sys
import uuid

from pyqueue.demo import add, fetch_data, flaky, poison

if __name__ == "__main__":
    run_id = sys.argv[1] if len(sys.argv) > 1 else uuid.uuid4().hex[:8]
    print(f"demo run {run_id}")
    print("add        ->", add.delay(2, 3))
    print("fetch_data ->", fetch_data.delay("demo"))
    print("flaky      ->", flaky.delay(run_id))
    print("poison     ->", poison.delay(run_id))
