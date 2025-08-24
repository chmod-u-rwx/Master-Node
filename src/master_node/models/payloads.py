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

class JobRequestPayload(BaseModel):
    request_id: UUID = Field(...)
    method: Optional[MethodEnum] = Field(None, description="HTTP method")
    headers: Optional[Dict[str, Any]] = Field(default_factory=dict, description="HTTP headers")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query parameters")
    body: Dict[str, Any] = Field(..., description="Main job data as a JSON object")

class JobResponsePayload(BaseModel):
    request_id: UUID = Field(...)
    status: str = Field(..., description='"ok" or "error"')
    result: Dict[str, Any] = Field(default_factory=dict, description="User's actual output in JSON")
    error: Optional[str] = Field(None, description="Error details if status='error'")
    meta: Dict[str, Any] = Field(default_factory=dict)