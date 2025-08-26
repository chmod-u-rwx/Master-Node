import asyncio
import json
from typing import Any, Awaitable, Callable, Dict
from uuid import UUID
from asyncio import Future
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from src.master_node.models.payloads import (
    JobRequestPayload,
    JobResponsePayload,
    WebsocketMessage,
    MessageType
)

# ---- Exceptions ----

class WorkerNotConnectedError(Exception):
    ...

class WorkerCommunicationError(Exception):
    ...

class InvalidWorkerResponseError(Exception):
    ...

class RPCRequestTimeoutError(asyncio.TimeoutError):
    ...

# ---- Master Node Server ----

class MasterNodeWebsocketServerService:
    def __init__(self):
        self.connections: Dict[str, WebSocket] = {}
        self.pending_requests: Dict[str, asyncio.Future[Any]] = {}
        self._handler: Dict[MessageType, Callable[[UUID, WebsocketMessage], Awaitable[None]]] = {
            MessageType.HEARTBEAT: self._handle_heartbeat,
            MessageType.STATUS_UPDATE: self._handle_status_update,
            MessageType.TASK_RESULT: self._handle_task_result,
            MessageType.JOB_RESPONSE: self._handle_job_response,
        }
    
    async def connect(self, worker_id: UUID, websocket: WebSocket):
        await websocket.accept()
        worker_key = str(worker_id)
        self.connections[worker_key] = websocket
    
        try:
            await self._handle_worker_websocket_message(worker_id, websocket)
        except WebSocketDisconnect:
            print(f"Worker {worker_id} disconnected normally")
        except Exception as e:
            print(f"Worker {worker_id} disconnected with an error: {e}")
        finally:
            if str(worker_id) in self.connections:
                del self.connections[str(worker_id)]
                print(f"Cleaned up connection for worker {worker_id}")
    
    async def _handle_worker_websocket_message(
        self,
        worker_id: UUID,
        websocket: WebSocket
    ):
        while True:
            try:
                raw_message = await websocket.receive_json()
                await self._process_message(worker_id, websocket, raw_message)
            except json.JSONDecodeError:
                await self._send_error_response(websocket, "Invalid JSON format")
            except WebSocketDisconnect:
                break
            except Exception as e:
                print(f"Unexpected error processing message from {worker_id}: {e}")
                await self._send_error_response(websocket, "Internal server error")
    
    async def _process_message(
        self,
        worker_id: UUID,
        websocket: WebSocket,
        raw_message: Dict[str, Any]
    ) -> None:
        """Process a single message from worker."""

        message = self._parse_message(raw_message)
        if message is None:
            await self._send_error_response(websocket, "Invalid message format")
            return
        
        handler = self._handler.get(message.type)
        if handler is None:
            await self._send_error_response(websocket, "Unknown message type")
            return
        
        try:
            await handler(worker_id, message)
        except Exception as e:
            print(f"Handler error for {message.type} from {worker_id}: {e}")
            await self._send_error_response(websocket, "Handler execution failed")
    
    def _parse_message(
        self,
        raw_message: Dict[str, Any]
    ) -> WebsocketMessage | None:
        """Parse raw message into WebsocketMessage. Returns None if invalid."""
        
        try:
            return WebsocketMessage(**raw_message)
        except ValidationError as e:
            print(f"Message validation failed: {e}")
            return None
    
    async def _send_error_response(self, websocket: WebSocket, error_message: str) -> None:
        """Send standardized error response to worker."""
        
        error_response: Dict[str, Any] = {
            "type": MessageType.ERROR,
            "payloads": {"error": error_message}
        }
        try:
            await websocket.send_json(error_response)
        except Exception as e:
            print(f"Failed to send error response: {e}")

    async def send_job_rpc_to_worker_node(
        self,
        worker_id: UUID,
        job_payload: JobRequestPayload,
        timeout: float = 30.0
    ) -> JobResponsePayload:
        
        if not self.is_worker_connected(worker_id):
            raise WorkerNotConnectedError(f"Worker {worker_id} not connected")
        
        ws = self._get_websocket_or_raise(worker_id)
        
        request_id = job_payload.request_id
        
        response_future: Future[Any] = asyncio.Future()
        self.pending_requests[str(request_id)] = response_future
        
        ws_message = WebsocketMessage(
            type=MessageType.JOB_REQUEST,
            request_id=job_payload.request_id,
            payloads=job_payload.model_dump(),
        )
        
        try:
            await ws.send_json(ws_message.model_dump())
            response = await asyncio.wait_for(response_future, timeout=timeout)
            
            return JobResponsePayload(**response)
        
        except ValidationError as ve:
            raise InvalidWorkerResponseError(f"Invalid response from worker: {ve}")
        except asyncio.TimeoutError as e:
            raise RPCRequestTimeoutError(f"Worker {worker_id} did not respond within {timeout} seconds") from e
        except Exception as e:
            raise RuntimeError(f"Failed to send job request to worker {worker_id}: {e}")
        
        finally:
            self.pending_requests.pop(str(request_id), None)
    
    # ---- Inbound Handler ----
    
    async def _handle_job_response(
        self,
        worker_id: UUID,
        ws_message: WebsocketMessage
    ):
        request_id = str(ws_message.request_id) if ws_message.request_id else None
        if not request_id or request_id not in self.pending_requests:
            print(f"Dangling job response from {worker_id} with request_id={ws_message.request_id}")
            return
        
        future = self.pending_requests[request_id]
        if future.done():
            return
        
        try:
            payload = JobResponsePayload(**ws_message.payloads)
            future.set_result(payload)
        except ValidationError as ve:
            future.set_exception(InvalidWorkerResponseError(f"Invalid response payload: {ve}"))
            return
    
    async def _handle_heartbeat(self, worker_id: UUID, msg: WebsocketMessage):
        print(f"Heartbeat from worker {worker_id}")
    
    async def _handle_status_update(self, worker_id: UUID, msg: WebsocketMessage):
        print(f"Status update from worker {worker_id}: {msg.payloads}")

    async def _handle_task_result(self, worker_id: UUID, msg: WebsocketMessage):
        print(f"Task result from worker {worker_id}: {msg.payloads}")
    
    # ---- Send to Worker ----
    
    async def send_to_worker(self, worker_id: UUID, ws_message: WebsocketMessage):
        """
        Send arbitrary data to worker (fire-and-forget style)
        """
        
        worker_key = str(worker_id)
        ws = self.connections.get(worker_key)
        
        if not ws:
            print(f"Worker {worker_id} not connected")
            raise RuntimeError(f"Worker {worker_id} not connected")
        
        try:
            await self.send_websocket_message_to_worker(worker_id, ws_message)
        except Exception as e:
            print(f"Failed to send to worker {worker_id}: {e}")
            if worker_key in self.connections:
                raise RuntimeError(f"Failed to send to worker {worker_id}: {e}")
    
    async def send_websocket_message_to_worker(
        self,
        worker_id: UUID,
        ws_message: WebsocketMessage
    ):
        ws = self._get_websocket_or_raise(worker_id)
        
        try:
            message_data = ws_message.model_dump()
            await ws.send_json(message_data)
        except Exception as e:
            self.connections.pop(str(worker_id), None)
            raise WorkerCommunicationError(f"Failed to send to worker {worker_id}: {e}")

    # ----- Utilities -----
    
    def _get_websocket_or_raise(self, worker_id: UUID) -> WebSocket:
        ws = self.connections.get(str(worker_id))
        if not ws:
            raise WorkerNotConnectedError(f"Worker {worker_id} not connected")
        return ws
    
    def get_connected_workers(self) -> list[str]:
        """Return list of connected worker IDs"""
        
        return list(self.connections.keys())
    
    def is_worker_connected(self, worker_id: UUID) -> bool:
        """Check if worker is connected"""
        
        return str(worker_id) in self.connections
    
    def get_pending_requests_count(self) -> int:
        """Get count of pending RPC requests"""
        
        return len(self.pending_requests)