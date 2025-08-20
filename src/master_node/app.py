from fastapi import FastAPI, WebSocket
from uuid import UUID
from .services.master_node_websocket_server_service import MasterNodeWebsocketServerService

app = FastAPI()
ws_service = MasterNodeWebsocketServerService()

@app.websocket("/ws/connect/{worker_id}")
async def master_node_websocket_connect(websocket: WebSocket, worker_id: UUID):
    await ws_service.connect(worker_id, websocket)
    print(f"Worker node: {worker_id}")