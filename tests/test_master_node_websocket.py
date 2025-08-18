import json
from typing import Any
from uuid import uuid4
import pytest
import websockets

WS_URL = "ws://localhost:8000/ws/{}"

@pytest.mark.asyncio
async def test_websocket_connection_and_valid_json():
    worker_id = str(uuid4())
    uri = WS_URL.format(worker_id)
    async with websockets.connect(uri) as websocket:
        message = json.dumps({"type": "heartbeat", "alive": "alive"})
        await websocket.send(message)
        await websocket.close()

@pytest.mark.asyncio
async def test_websocket_invalid_json():
    worker_id = str(uuid4())
    uri = WS_URL.format(worker_id)
    async with websockets.connect(uri) as websocket:
        await websocket.send("not a json string")
        await websocket.close()

@pytest.mark.asyncio
async def test_multiple_worker_connections():
    worker_ids = [str(uuid4()) for _ in range(3)]
    websockets_list: list[Any] = []
    
    for w_id in worker_ids:
        uri = WS_URL.format(w_id)
        ws = await websockets.connect(uri)
        websockets_list.append(ws)
    
    for ws in websockets_list:
        assert ws.state.name == "OPEN"
    
    for ws in websockets_list:
        await ws.close()

@pytest.mark.asyncio
async def test_send_large_message():
    worker_id = str(uuid4())
    uri = WS_URL.format(worker_id)
    async with websockets.connect(uri) as websocket:
        large_message = json.dumps({"type": "status_update", "status": "x" * 10000})
        await websocket.send(large_message)
        await websocket.close()

@pytest.mark.asyncio
async def test_reconnect_same_worker_id():
    worker_id = str(uuid4())
    uri = WS_URL.format(worker_id)
    
    # First connection
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({"type": "heartbeat"}))
        await websocket.close()
    # Reconnect with same worker_id
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({"type": "heartbeat"}))
        await websocket.close()

@pytest.mark.asyncio
async def test_connection_refused():
    bad_uri = "ws://localhost:8765/ws/{}".format(uuid4())
    with pytest.raises(Exception):
        await websockets.connect(bad_uri)

@pytest.mark.asyncio
async def test_send_non_string_message():
    worker_id = str(uuid4())
    uri = WS_URL.format(worker_id)
    async with websockets.connect(uri) as websocket:
        with pytest.raises(TypeError):
            await websocket.send(12345) # type: ignore
        await websocket.close()