import time
from dataclasses import dataclass, field
from uuid import UUID, uuid7

import msgpack


@dataclass
class TaskMessage:
    task_name: str
    retries: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    task_id: UUID = field(default_factory=uuid7)
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        data = {
            "task_name": self.task_name,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "task_id": str(self.task_id),
            "args": self.args,
            "kwargs": self.kwargs,
        }
        return msgpack.packb(data)

    @classmethod
    def from_bytes(cls, data: bytes):
        unpacked_data = msgpack.unpackb(data, raw=False)
        return cls(
            task_name=unpacked_data["task_name"],
            retries=unpacked_data["retries"],
            max_retries=unpacked_data["max_retries"],
            created_at=unpacked_data["created_at"],
            task_id=UUID(unpacked_data["task_id"]),
            args=unpacked_data["args"],
            kwargs=unpacked_data["kwargs"],
        )
