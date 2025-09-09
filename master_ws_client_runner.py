import asyncio
from uuid import UUID
from src.master_node.services.websocket_client_service import master_client_ws

async def main():
    # Connect to ingress router
    await master_client_ws.connect(max_reconnect_attempts=3)
    # Start listening for messages
    await master_client_ws.listen_for_messages()

# if __name__ == "__main__":
#     asyncio.run(main())