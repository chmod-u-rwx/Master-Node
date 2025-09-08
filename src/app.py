from fastapi import FastAPI, WebSocket
from uuid import UUID
from src.master_node.services.master_node_websocket_server_service import master_node_ws_server
app = FastAPI()

@app.websocket("/ws/connect/{worker_id}")
async def master_node_websocket_connect(websocket: WebSocket, worker_id: UUID):
    await master_node_ws_server.connect(worker_id, websocket)
    print(f"Worker node: {worker_id}")