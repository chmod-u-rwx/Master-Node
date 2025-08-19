import json
from typing import Any, Dict
from uuid import UUID
from fastapi import WebSocket, WebSocketDisconnect

class MasterNodeWebsocketServerService:
    def __init__(self):
        self.connections: Dict[str, WebSocket] = {}
    
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
    
    async def handle_worker_message(self, worker_id: UUID, message: dict[str, Any]):
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
                del self.connections[worker_key]
            raise RuntimeError(f"Fauked ti send to worker {worker_id}: {e}")