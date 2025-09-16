from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from enum import Enum
import uuid
from datetime import datetime


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkerStatus(str, Enum):
    ACTIVE = "active"
    IDLE = "idle"
    OFFLINE = "offline"


class Task(BaseModel):
    id: str = None
    function_code: bytes
    input_data: List[Any]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    
    def __init__(self, **data):
        if data.get('id') is None:
            data['id'] = str(uuid.uuid4())
        if data.get('created_at') is None:
            data['created_at'] = datetime.utcnow()
        super().__init__(**data)


class WorkUnit(BaseModel):
    id: str = None
    task_id: str
    data_chunk: Any
    worker_id: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    assigned_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    
    def __init__(self, **data):
        if data.get('id') is None:
            data['id'] = str(uuid.uuid4())
        super().__init__(**data)


class Worker(BaseModel):
    id: str = None
    host: str
    port: int
    status: WorkerStatus = WorkerStatus.IDLE
    last_heartbeat: datetime = None
    current_task: Optional[str] = None
    
    def __init__(self, **data):
        if data.get('id') is None:
            data['id'] = str(uuid.uuid4())
        if data.get('last_heartbeat') is None:
            data['last_heartbeat'] = datetime.utcnow()
        super().__init__(**data)


class TaskSubmission(BaseModel):
    function_code: str
    input_data: List[Any]


class TaskResult(BaseModel):
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None