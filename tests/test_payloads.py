from uuid import uuid4

import pytest
from src.master_node.models.payloads import JobRequestPayload, JobResponsePayload, MethodEnum

def test_job_request_payload_minimal():
    payload = JobRequestPayload(
        request_id=uuid4(),
        body={"task": "process", "value": 42}
    ) # type: ignore
    
    assert isinstance(payload.request_id, type(uuid4()))
    assert payload.method is None
    assert payload.headers == {}
    assert payload.params == {}
    assert payload.body == {"task": "process", "value": 42}

def test_job_payload_full():
    payload = JobRequestPayload(
        request_id=uuid4(),
        method=MethodEnum.POST,
        headers={"Authorization": "Bearer token"},
        params={"q": "search"},
        body={"task": "analyze", "data": [1, 2, 3]}
    )
    
    assert payload.method == MethodEnum.POST
    
    assert payload.headers is not None
    assert payload.headers["Authorization"] == "Bearer token"
    
    assert payload.params is not None
    assert payload.params["q"] == "search"
    assert payload.body["task"] == "analyze"

def test_job_response_payload_ok():
    payload = JobResponsePayload(
        request_id=uuid4(),
        status="ok",
        result={"output": 123},
        error=None,
        meta={"runtime_ms": 10, "exit_code": 0}
    )
    assert payload.status == "ok"
    assert payload.result["output"] == 123
    assert payload.error is None
    assert payload.meta["exit_code"] == 0

def test_job_response_payload_error():
    payload = JobResponsePayload(
        request_id=uuid4(),
        status="error",
        result={},
        error="Something went wrong",
        meta={"runtime_ms": 5, "exit_code": 1}
    )
    assert payload.status == "error"
    assert payload.error == "Something went wrong"
    assert payload.meta["exit_code"] == 1

def test_job_request_payload_invalid_method():
    with pytest.raises(ValueError):
        JobRequestPayload(
            request_id=uuid4(),
            method="INVALID",  # Not in MethodEnum # type: ignore
            body={}
        )