import time
from uuid import UUID

from pyqueue.task import REGISTRY, TaskMessage, task


def test_task_message_initialization():
    """Verify that a TaskMessage instance initializes
    correctly with all required arguments."""
    now = time.time()
    task = TaskMessage(
        task_name="send_email",
        args=["user@example.com"],
        kwargs={"subject": "Welcome"},
        retries=0,
        created_at=now,
    )

    assert task.task_name == "send_email"
    assert task.args == ["user@example.com"]
    assert task.kwargs == {"subject": "Welcome"}
    assert task.retries == 0
    assert task.created_at == now


def test_task_message_defaults():
    """Verify that default values for max_retries and task_id are applied properly."""
    task = TaskMessage(
        task_name="process_video",
        args=[],
        kwargs={},
        retries=2,
        created_at=time.time(),
    )

    # Check that task_id is automatically generated as a UUID instance
    assert isinstance(task.task_id, UUID)


def test_task_message_unique_ids():
    """Verify that every new instance gets a unique automatically generated UUID."""
    task_1 = TaskMessage(
        "task",
        0,
        0,
        time.time(),
    )
    task_2 = TaskMessage(
        "task",
        0,
        0,
        time.time(),
    )

    assert task_1.task_id != task_2.task_id


def test_task_message_explicit_optional_values():
    """Verify that defaults can be overridden with explicit values."""
    custom_uuid = UUID("018af123-4567-789a-bcde-f0123456789a")
    task = TaskMessage(
        task_name="cleanup",
        args=[],
        kwargs={},
        retries=1,
        created_at=time.time(),
        max_retries=5,
        task_id=custom_uuid,
    )

    assert task.max_retries == 5
    assert task.task_id == custom_uuid


def test_task_message_roundtrip():
    original = TaskMessage(task_name="test_func", args=[1, "hello"], kwargs={"x": True})
    raw_bytes = original.to_bytes()
    assert isinstance(raw_bytes, bytes)

    restored = TaskMessage.from_bytes(raw_bytes)
    assert restored.task_name == original.task_name
    assert restored.task_id == original.task_id
    assert restored.args == original.args
    assert restored.kwargs == original.kwargs
    assert restored.retries == 0
    assert restored.max_retries == 3


def test_task_decorator_and_registry():
    @task
    def add(a, b):
        return a + b

    # Direct execution works
    assert add(2, 3) == 5

    # Exists in registry
    expected_key = f"{add.__module__}.add"
    assert expected_key in REGISTRY
    assert REGISTRY[expected_key] is add

    # .delay returns an envelope
    # msg = add.delay(2, 3)
    # assert isinstance(msg, UUID)
    # assert msg.task_name == expected_key
    # assert msg.args == [2, 3]
