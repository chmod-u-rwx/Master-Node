from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID
from pydantic import BaseModel, Field

class MethodEnum(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    RUN = "RUN"  # Custom method for jobs

class MessageType(str, Enum):
    HEARTBEAT = "heartbeat"
    JOB_REQUEST = "job_request"
    JOB_RESPONSE = "job_response"
    STATUS_UPDATE = "status_update"
    TASK_RESULT = "task_result"
    ERROR = "error"

class WebsocketMessage(BaseModel):
    request_id: Optional[UUID] = Field(default=None)
    type: MessageType = Field(...)
    payloads: Any = Field(...)

class JobRequestPayload(BaseModel):
    request_id: UUID = Field(...)
    job_id: str
    master_id: UUID = Field(...)
    worker_id: UUID = Field(...)
    method: Optional[MethodEnum] = Field(None, description="HTTP method")
    path: str = Field(...)
    headers: Optional[Dict[str, Any]] = Field(default_factory=dict, description="HTTP headers")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query parameters")
    body: Any | None = Field(..., description="Main job data as a JSON object")

class JobResponsePayload(BaseModel):
    request_id: UUID = Field(...)
    job_id: str
    master_id: UUID = Field(...)
    worker_id: UUID = Field(...)
    status_code: int = Field(...)
    body: Any = Field(default_factory=dict, description="Output may be error or not")
    meta: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, Any] = Field(default_factory=dict)