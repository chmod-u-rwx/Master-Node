from fastapi import FastAPI, WebSocket
from uuid import UUID
from .services.websocket_server_service import WebsocketServerService

app = FastAPI()
ws_service = WebsocketServerService()

@app.websocket("/ws/{worker_id}")
async def websocket_endpoints(websocket: WebSocket, worker_id: UUID):
    await ws_service.connect(worker_id, websocket)
    print(f"Worker node: {worker_id}")