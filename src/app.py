import asyncio
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
from uuid import UUID
from src.master_node.services.master_node_websocket_server_service import master_node_ws_server
from master_ws_client_runner import main


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("startup")
    task = asyncio.create_task(main())   # run main in background
    yield
    print("shutdown")
    task.cancel()  # stop the task

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/connect/{worker_id}")
async def master_node_websocket_connect(websocket: WebSocket, worker_id: UUID):
    await master_node_ws_server.connect(worker_id, websocket)
    print(f"Worker node: {worker_id}")