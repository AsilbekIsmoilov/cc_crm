from __future__ import annotations
import logging
import threading

_local = threading.local()

def set_request_id(request_id: str | None = None) -> None:
    _local.request_id = request_id

def get_request_id() -> str | None:
    return getattr(_local, "request_id", None)

def set_log_user(user) -> None:
    _local.log_user = getattr(user, "username", None) or (str(user) if user else None)

def get_log_user() -> str | None:
    return getattr(_local, "log_user", None)

class EnsureWorkerFilter(logging.Filter):
    def __init__(self, name: str = "", default_worker: str = "-"):
        super().__init__(name)
        self.default_worker = default_worker

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker"):
            record.worker = self.default_worker
        rid = get_request_id()
        if rid is not None and not hasattr(record, "request_id"):
            record.request_id = rid
        u = get_log_user()
        if u is not None and not hasattr(record, "log_user"):
            record.log_user = u
        return True

class SafeWorkerFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "worker"):
            record.worker = "-"
        return super().format(record)

SafeWorkerFilter = EnsureWorkerFilter
