# import asyncio
# from uuid import UUID
# from src.master_node.services.websocket_client_service import master_client_ws

# async def main():
#     # Connect to ingress router
#     await master_client_ws.connect(max_reconnect_attempts=3)
#     # Start listening for messages
#     await master_client_ws.listen_for_messages()

# if __name__ == "__main__":
#     asyncio.run(main())


import asyncio
from uuid import UUID
from fastapi import FastAPI, WebSocket
import uvicorn
from src.master_node.services.master_node_websocket_server_service import master_node_ws_server
from src.master_node.services.websocket_client_service import WebsocketClientService
from src.master_node.config import INGRESS_ROUTER_URI

MASTER_ID = UUID("550e8400-e29b-41d4-a716-446655440000")

app = FastAPI()

# --- Worker WebSocket server endpoint ---
@app.websocket("/ws/connect/{worker_id}")
async def master_node_websocket_connect(websocket: WebSocket, worker_id: UUID):
    await master_node_ws_server.connect(worker_id, websocket)

# --- Run FastAPI server in asyncio ---
async def run_fastapi_server():
    config = uvicorn.Config(app, host="0.0.0.0", port=8020, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

# --- Connect to ingress router ---
async def run_master_client():
    master_client_ws = WebsocketClientService(master_id=MASTER_ID)
    await master_client_ws.connect(max_reconnect_attempts=3)
    await master_client_ws.listen_for_messages()

# --- Main async entry point ---
async def main():
    # Run server and client concurrently
    await asyncio.gather(
        run_fastapi_server(),
        run_master_client(),
    )

if __name__ == "__main__":
    asyncio.run(main())
