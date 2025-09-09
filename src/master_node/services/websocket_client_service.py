import json
# import httpx
import websockets
import asyncio
from typing import Any, Dict, Optional
from uuid import uuid4, UUID
from websockets.exceptions import ConnectionClosed, WebSocketException
from src.master_node.config import INGRESS_ROUTER_URI
# from src.master_node.models.ingress_router import IngressRouter
from src.master_node.models.payloads import WebsocketMessage, MessageType, JobRequestPayload, JobResponsePayload
from src.master_node.services.master_node_websocket_server_service import master_node_ws_server

class MasterNodeDiscoveryError(Exception):
    ...

class MasterNodeNotFound(MasterNodeDiscoveryError):
    ...

class MasterNodeServerError(MasterNodeDiscoveryError):
    ...

class MasterNodeInvalidResponse(MasterNodeDiscoveryError):
    ...

class WebsocketClientService:
    def __init__(self,
            master_id: UUID,
            max_reconnect_attempts: int = 3,
        ):
        self.master_id = str(master_id)
        self.websocket = None
        self.max_reconnect_attempts = max_reconnect_attempts
        self.current_websocket_url = None
    
    async def connect(
        self,
        max_reconnect_attempts: int,
        max_rediscoveries: int = 2,
        websocket_url: Optional[str] = None
    ) -> None:
        if not websocket_url:
            websocket_url = await self.discover_ingress_router()
        
        current_address = websocket_url
        rediscoveries = 0
        
        while rediscoveries <= max_rediscoveries:
            rediscoveries += 1
            for attempt in range(1, self.max_reconnect_attempts + 1):
                try:
                    self.websocket = await websockets.connect(current_address)
                    self.current_websocket_url = current_address
                    return

                except ConnectionClosed:
                    if attempt == self.max_reconnect_attempts:
                        new_address = await self.discover_ingress_router()
                        if new_address != current_address:
                            current_address = new_address

                        break 
                        
                except Exception as e:
                    print(f"Unexpected error during WebSocket connection (attempt {attempt}): {e}")
        else:
            raise ConnectionError(f"Failed to connect after {max_reconnect_attempts} attempts")
    
    async def listen_for_messages(self) -> None:
        if not self.websocket:
            raise RuntimeError("Not connected to WebSocket server")
        
        while self.websocket:
            message: str | bytes = ""
            try:
                message = await self.websocket.recv()
                data = json.loads(message)
                print(f"Received JSON message: {data}")

                # Serialize message
                data = WebsocketMessage(**data)
                if data.type == MessageType.JOB_REQUEST:
                    await self.handle_job_rpc_request(JobRequestPayload(**data.payloads))

            except (ConnectionClosed, WebSocketException):
                await self.disconnect()
                await self.connect(self.max_reconnect_attempts)
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message}")
            except Exception:
                await self.disconnect()
                raise
            
        await self.disconnect()

    async def handle_job_rpc_request(self, job_request: JobRequestPayload):
        print(master_node_ws_server.get_connected_workers())
        print("handling job in client")
        response = await master_node_ws_server.send_job_rpc_to_worker_node(
            worker_id=job_request.worker_id,
            job_payload=job_request
		)

        message = WebsocketMessage(
            request_id=job_request.request_id,
            type=MessageType.JOB_RESPONSE,
            payloads=response
        )

        await self.send_message(message=message)
    
    async def send_message(self, message: WebsocketMessage) -> None:
        if not self.websocket:
            raise RuntimeError("Not connected to WebSocket server")
        
        max_send_attempts = 2
        current_attempts = 0
        
        while current_attempts < max_send_attempts:
            try:
                await self.websocket.send(json.dumps(message.model_dump(mode="json")))
                print(f"Sent message: {message}")
                return
            
            except (ConnectionClosed, WebSocketException):
                await self.disconnect()
                await self.connect(self.max_reconnect_attempts)
            except Exception:
                current_attempts += 1
                if current_attempts < max_send_attempts:
                    continue
                raise
    
    async def disconnect(self) -> None:
        if self.websocket:
            try:
                await self.websocket.close()
            finally:
                self.websocket = None
    
    def is_connected(self) -> bool:
        """
        Check if the websocket is currently connected
        """
        
        return self.websocket is not None
    
    async def discover_ingress_router(self) -> str:
        # async with httpx.AsyncClient() as client:
        #     try:
        #         response = await client.get(f"{CORE_API_URI}/master-node/discover")
        #         if response.status_code == 404:
        #             raise MasterNodeNotFound("Master node discovery endpoint returned 404 Not Found")
        #         elif 500 <= response.status_code < 600:
        #             raise MasterNodeServerError(f"Master node discovery failed with status {response.status_code}")
        #         response.raise_for_status()
        #         ingress_router_data = response.json()
        #         try:
        #             ingress_router = IngressRouter(**ingress_router_data)
        #         except Exception as e:
        #             raise MasterNodeInvalidResponse(f"Invalid master node data: {e}")
                
        #         master_address = str(ingress_router.ingress_address)
        #     except httpx.RequestError as e:
        #         raise MasterNodeDiscoveryError(f"HTTP request failed: {e}") from e
            
            websocket_url = f"ws://{INGRESS_ROUTER_URI}/ws/connect/{self.master_id}"
            print(f"Discovered master node websocket at: {websocket_url}")
            return websocket_url

master_client_ws = WebsocketClientService(master_id=UUID("550e8400-e29b-41d4-a716-446655440000"))