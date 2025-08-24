import asyncio
import json
import uuid
from typing import Any, Dict
from uuid import UUID
from asyncio import Future
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from src.master_node.models.payloads import JobRequestPayload, JobResponsePayload

class MasterNodeWebsocketServerService:
    def __init__(self):
        self.connections: Dict[str, WebSocket] = {}
        self.pending_requests: Dict[str, asyncio.Future[Any]] = {}
    
    async def connect(self, worker_id: UUID, websocket: WebSocket):
        await websocket.accept()
        self.connections[str(worker_id)] = websocket
        print(f"Worker {worker_id} connected.")
    
        try:
            while True:
                try:
                    message = await websocket.receive_json()
                    print(f"From {worker_id}: {message}")
                    await self.handle_worker_message(worker_id, message)
                except json.JSONDecodeError:
                    print(f"Invalid JSON from worker: {worker_id}")
                    await websocket.send_json({"error": "Invalid JSON format"})
                    continue 
        except WebSocketDisconnect:
            print(f"Worker {worker_id} disconnected normally")
        except Exception as e:
            print(f"Worker {worker_id} disconnected with an error: {e}")
        finally:
            if str(worker_id) in self.connections:
                del self.connections[str(worker_id)]
                print(f"Cleaned up connection for worker {worker_id}")
    
    async def send_job_rpc_to_worker_node(
        self,
        worker_id: UUID,
        job_payload: JobRequestPayload,
        timeout: float = 30.0
    ) -> JobResponsePayload:
        worker_key = str(worker_id)
        ws = self.connections.get(worker_key)
        
        if not ws:
            raise RuntimeError(f"Worker {worker_id} not connected")
        
        if not getattr(job_payload, "request_id", None):
            job_payload.request_id = uuid.uuid4()
        
        request_id = str(job_payload.request_id)
        response_future: Future[Any] = asyncio.Future()
        self.pending_requests[request_id] = response_future
        
        try:
            await ws.send_json(job_payload.model_dump())
            print(f"Sent job request to worker {worker_id}: {job_payload.method or 'N/A'}")
            
            response = await asyncio.wait_for(response_future, timeout=timeout)
            
            try:
                return JobResponsePayload(**response)
            except ValidationError as e:
                print(f"Invalid response format from worker {worker_id}: {e}")
                raise RuntimeError(f"Worker {worker_id} returned invalid response format") from e

        except asyncio.TimeoutError:
            print(f"Job request to worker {worker_id} timed out after {timeout}s")
            raise asyncio.TimeoutError(f"Worker {worker_id} did not respond within {timeout} seconds")
        except asyncio.CancelledError:
            print(f"Job request to worker {worker_id} was cancelled")
            raise
        
        except Exception as e:
                print(f"Failed to send job request to worker {worker_id}: {e}")
                raise RuntimeError(f"Failed to send job request to worker {worker_id}: {e}")
        
        finally:
            self.pending_requests.pop(request_id, None)
    
    async def handle_worker_message(self, worker_id: UUID, message: dict[str, Any]):
        request_id = message.get("request_id")
        if request_id and request_id in self.pending_requests:
            future = self.pending_requests[request_id]
            if not future.done():
                try:
                    JobResponsePayload(**message)
                    future.set_result(message)
                    print(f"Resolved pending request {request_id} from worker {worker_id}")
                except ValidationError as e:
                    print(f"Invalid response format for request {request_id}: {e}")
                    future.set_exception(e)
                except Exception as e:
                    print(f"Error processing response for request {request_id}: {e}")
                    future.set_exception(e)
            return
        
        message_type = message.get("type")
        if not message_type:
            print(f"Missing 'type' in message from worker {worker_id}: {message}")
            ws = self.connections.get(str(worker_id))
            if ws:
                await ws.send_json({"error": "Missing 'type' in message"})
            return
        
        if message_type == "heartbeat":
            print(f"Heartbeat from worker {worker_id}")
        elif message_type == "task_result":
            print(f"Task result from worker {worker_id}: {message.get('data')}")
        elif message_type == "status_update":
            print(f"Status update from worker {worker_id}: {message.get('status')}")
        else:
            print(f"Unknown message type from worker {worker_id}: {message}")
            ws = self.connections.get(str(worker_id))
            if ws:
                await ws.send_json({"error": "Unknown message type"})
        
    async def send_to_worker(self, worker_id: UUID, data: dict[str, Any]):
        """
        Send arbitrary data to worker (fire-and-forget style)

        Args:
            worker_id (UUID): UUID of target
            data (dict[str, Any]): Data to send

        Raises:
            RuntimeError: If worker not connected or send fails
        """
        
        worker_key = str(worker_id)
        ws = self.connections.get(worker_key)
        
        if not ws:
            print(f"Worker {worker_id} not connected")
            raise RuntimeError(f"Worker {worker_id} not connected")
        
        try:
            await ws.send_json(data)
            print(f"Sent to {worker_id}: {data}")
        except Exception as e:
            print(f"Failed to send to worker {worker_id}: {e}")
            if worker_key in self.connections:
                raise RuntimeError(f"Failed to send to worker {worker_id}: {e}")

    def get_connected_workers(self) -> list[str]:
        """Return list of connected worker IDs"""
        
        return list(self.connections.keys())
    
    def is_worker_connected(self, worker_id: UUID) -> bool:
        """Check if worker is connected"""
        
        return str(worker_id) in self.connections
    
    def get_pending_requests_count(self) -> int:
        """Get count of pending RPC requests"""
        
        return len(self.pending_requests)